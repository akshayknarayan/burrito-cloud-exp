use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use openssh::Session;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tracing::{debug, info, instrument, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;
use tsunami::providers::{aws, azure, baremetal};
use tsunami::Tsunami;

#[derive(Debug, Clone, StructOpt)]
struct Opt {
    /// Node config
    #[structopt(short, long)]
    cfg: PathBuf,

    /// Location of the bench binary to copy
    #[structopt(short, long)]
    bench_bin: PathBuf,
    /// Location of the experiment script to copy
    #[structopt(short, long)]
    script: PathBuf,
}

async fn with_launcher(
    launcher: &mut impl tsunami::Tsunami,
    machine_name: &str,
    script_remote_path: &Path,
    bench_remote_path: &Path,
    prov: &str,
) -> Result<(), Report> {
    let conns = launcher.connect_all().await?;
    let vm = conns.get(machine_name).unwrap();
    do_exp(&vm.ssh, script_remote_path, bench_remote_path, prov).await
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
enum Node {
    Aws { region: String },
    Azure { region: String },
    Baremetal { ip: String, user: String },
}

impl Node {
    #[instrument]
    async fn run(self, bench_bin: PathBuf, script: PathBuf) -> Result<(), Report> {
        let bench_remote_path = Path::new(bench_bin.file_name().unwrap()).to_path_buf();
        let script_remote_path = Path::new(script.file_name().unwrap()).to_path_buf();

        info!("starting machines");

        const MACHINE_NAME: &str = "burrito-test-machine";
        let chmod_cmd = format!("chmod +x {}", bench_remote_path.to_str().unwrap());
        match self {
            Node::Aws { region } => {
                let srp = script_remote_path.clone();
                let brp = bench_remote_path.clone();
                let mut aws_launcher = aws::Launcher::default();
                aws_launcher.set_mode(aws::LaunchMode::TrySpot { hours: 6 });
                let ami = ubuntu_ami::get_latest(
                    &region,
                    Some("focal"),
                    None,
                    Some("hvm:ebs-ssd"),
                    Some("amd64"),
                )
                .await
                .map_err(|e| eyre!(e))?;
                let m = aws::Setup::default()
                    .region(region.clone().parse()?, ami, "ubuntu")
                    .instance_type("t3.medium")
                    .setup(move |vm| {
                        let bench_remote_path = brp.clone();
                        let bench_bin = bench_bin.clone();
                        let script = script.clone();
                        let script_remote_path = srp.clone();
                        let chmod_cmd = chmod_cmd.clone();
                        Box::pin(async move {
                            install_deps(&vm.ssh).await?;
                            write_file(&vm.ssh, &bench_bin, bench_remote_path.as_path()).await?;
                            let ok = vm.ssh.shell(&chmod_cmd).status().await?;
                            ensure!(ok.success(), "chmod bench");
                            write_file(&vm.ssh, &script, script_remote_path.as_path()).await?;
                            Ok(())
                        })
                    });
                if let Err(e) = aws_launcher
                    .spawn(
                        vec![(MACHINE_NAME.to_owned(), m)],
                        Some(std::time::Duration::from_secs(180)),
                    )
                    .await
                {
                    aws_launcher.terminate_all().await?;
                    return Err(e);
                }

                //wait_for_continue();

                let res = with_launcher(
                    &mut aws_launcher,
                    MACHINE_NAME,
                    &script_remote_path,
                    bench_remote_path.as_path(),
                    "aws",
                )
                .await;
                aws_launcher.terminate_all().await?;
                res
            }
            Node::Azure { region: r } => {
                let srp = script_remote_path.clone();
                let brp = bench_remote_path.clone();
                let mut az_launcher = azure::Launcher::default();
                let m = azure::Setup::default()
                    .region(r.clone().parse()?)
                    .image("Canonical:0001-com-ubuntu-server-focal:20_04-lts:latest".to_owned())
                    .instance_type("Standard_B2ms".to_owned())
                    .setup(move |vm| {
                        let bench_remote_path = brp.clone();
                        let bench_bin = bench_bin.clone();
                        let script = script.clone();
                        let script_remote_path = srp.clone();
                        let chmod_cmd = chmod_cmd.clone();
                        Box::pin(async move {
                            install_deps(&vm.ssh).await?;
                            write_file(&vm.ssh, &bench_bin, bench_remote_path.as_path()).await?;
                            let ok = vm.ssh.shell(&chmod_cmd).status().await?;
                            ensure!(ok.success(), "chmod bench");
                            write_file(&vm.ssh, &script, script_remote_path.as_path()).await?;
                            Ok(())
                        })
                    });
                if let Err(e) = az_launcher
                    .spawn(vec![(MACHINE_NAME.to_owned(), m)], None)
                    .await
                {
                    az_launcher.terminate_all().await?;
                    return Err(e);
                }

                let res = with_launcher(
                    &mut az_launcher,
                    MACHINE_NAME,
                    &script_remote_path,
                    bench_remote_path.as_path(),
                    "azure",
                )
                .await;
                az_launcher.terminate_all().await?;
                res
            }
            Node::Baremetal { ip: i, user: u } => {
                let mut launcher = baremetal::Machine::default();
                let srp = script_remote_path.clone();
                let brp = bench_remote_path.clone();
                let m =
                    tsunami::providers::baremetal::Setup::new((i.as_str(), 22), Some(u.clone()))?
                        .setup(move |vm| {
                            let bench_remote_path = brp.clone();
                            let bench_bin = bench_bin.clone();
                            let script = script.clone();
                            let script_remote_path = srp.clone();
                            let chmod_cmd = chmod_cmd.clone();
                            Box::pin(async move {
                                install_deps(&vm.ssh).await?;
                                write_file(&vm.ssh, &bench_bin, bench_remote_path.as_path())
                                    .await?;
                                let ok = vm.ssh.shell(&chmod_cmd).status().await?;
                                ensure!(ok.success(), "chmod bench");
                                write_file(&vm.ssh, &script, script_remote_path.as_path()).await?;
                                Ok(())
                            })
                        });
                // termination doesn't matter here
                launcher
                    .spawn(vec![(MACHINE_NAME.to_owned(), m)], None)
                    .await?;
                let conns = launcher.connect_all().await?;
                let vm = conns.get(MACHINE_NAME).unwrap();
                do_exp(
                    &vm.ssh,
                    &script_remote_path,
                    bench_remote_path.as_path(),
                    "gcp",
                )
                .await
            }
        }
    }
}

async fn do_exp(
    ssh: &Session,
    script_remote_path: &Path,
    bench_remote_path: &Path,
    prov: &str,
) -> Result<(), Report> {
    let mut cmd = ssh.command("python3");
    cmd.arg(script_remote_path.to_str().unwrap());
    cmd.arg(Path::new(".").join(bench_remote_path).to_str().unwrap());
    cmd.arg(prov);
    info!(?cmd, "running");

    let out = cmd.output().await?;
    if !out.status.success() {
        warn!("script failed");
        println!("{}", String::from_utf8(out.stderr).unwrap());
    }

    tokio::fs::write(format!("{}.log", prov), out.stdout).await?;
    info!("done, getting files");
    let mut sftp = ssh.sftp();

    //let inter_req_times = [0, 25, 50, 75, 100];
    let inter_req_times = [75];
    let num_receivers = [1, 2, 5, 10];
    let num_groups = [0, 1, 2, 5, 10]; // + be
    let batch_sizes = [1, 5, 10];
    let batch_types = ["loop", "opt"];
    let impls = ["client", "service"];

    let mut fnames = vec![];
    for inter_req in &inter_req_times[..] {
        for batch_size in &batch_sizes[..] {
            for batch_type in &batch_types[..] {
                for rcvrs in &num_receivers[..] {
                    for imp in impls {
                        fnames.push(format!(
                            "exp-{}-be-{}ms-{}rcvrs-{}batch-{}-{}.data",
                            prov, inter_req, rcvrs, batch_size, batch_type, imp
                        ));
                    }

                    for grps in &num_groups[..] {
                        for imp in impls {
                            fnames.push(format!(
                                "exp-{}-ord:{}g-{}ms-{}rcvrs-{}batch-{}-{}.data",
                                prov, grps, inter_req, rcvrs, batch_size, batch_type, imp
                            ));
                        }
                    }
                }
            }
        }
    }

    //let fnames = ["transition-25ms-aws-ord5g.data"];
    let tot = fnames.len();
    let mut gotten = 0;
    for fname in &fnames[..] {
        let f = sftp.read_from(&fname).await;
        if let Err(err) = f {
            // the file not existing is not necessarily a problem, it's possible that experiment
            // was not run this time.
            warn!(?err, ?fname, "file error");
        } else {
            let mut f = f.unwrap();
            let mut local = tokio::fs::File::create(&fname).await?;
            tokio::io::copy(&mut f, &mut local).await?;
            f.close().await?;
            gotten += 1;
        }
    }

    info!(considered = ?tot, ?gotten, "done getting files");

    wait_for_continue();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    let subscriber = tracing_subscriber::registry();
    let subscriber = subscriber
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    let opt = Opt::from_args();
    info!(?opt, "starting");

    ensure!(
        opt.bench_bin.exists(),
        "Bench binary {:?} not found",
        opt.bench_bin
    );
    ensure!(
        opt.script.exists(),
        "Script path {:?} not found",
        opt.script
    );

    let cfg_file = std::fs::File::open(&opt.cfg).wrap_err(eyre!("Open cfg file {:?}", &opt.cfg))?;
    let nodes: Vec<Node> = serde_json::from_reader(cfg_file).wrap_err("parse cfg file json")?;

    for n in nodes {
        n.run(opt.bench_bin.clone(), opt.script.clone()).await?;
    }

    Ok(())
}

async fn write_file(vm: &Session, local_path: &Path, remote_path: &Path) -> Result<(), Report> {
    let mut sftp = vm.sftp();
    debug!(?local_path, ?remote_path, "writing file");
    let mut w = sftp
        .write_to(remote_path)
        .await
        .wrap_err("Open remote file for writing")?;
    let mut f = tokio::fs::File::open(local_path).await?;
    tokio::io::copy(&mut f, &mut w).await?;
    w.close().await?;
    Ok(())
}

fn wait_for_continue() {
    eprintln!("pausing for manual instance inspection, press enter to continue");

    use std::io::prelude::*;
    let stdin = std::io::stdin();
    let mut iterator = stdin.lock().lines();
    iterator.next().unwrap().unwrap();
}

async fn install_deps(ssh: &Session) -> Result<(), Report> {
    apt_install(ssh).await?;
    let st = ssh
        .shell("sudo pip3 install agenda")
        .status()
        .await
        .wrap_err("pip install")?;
    ensure!(st.success(), "pip install");
    Ok(())
}

async fn apt_install(ssh: &Session) -> Result<(), Report> {
    let mut count = 0;
    loop {
        count += 1;
        let res = async {
            let status = ssh
                .shell("sudo add-apt-repository -y ppa:redislabs/redis")
                .status()
                .await
                .wrap_err("redis repository add failed")?;
            ensure!(status.success(), "redis apt-add-repository failed");
            let status = ssh.shell(
                "sudo apt update && sudo DEBIAN_FRONTEND=noninteractive apt install -y python3-pip redis && sudo /etc/init.d/redis-server stop",
            ).status().await.wrap_err("apt install failed")?;
            ensure!(status.success(), "apt install failed");
            Ok(())
        }
        .await;

        if res.is_ok() {
            return res;
        } else {
            warn!(?res, "apt failed");
        }

        if count > 15 {
            return res;
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
