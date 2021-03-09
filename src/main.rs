use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use openssh::Session;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tracing::{debug, info, instrument};
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
) -> Result<(), Report> {
    let conns = launcher.connect_all().await?;
    let vm = conns.get(machine_name).unwrap();
    do_exp(&vm.ssh, &script_remote_path).await
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
        match self {
            Node::Aws { region } => {
                let srp = script_remote_path.clone();
                let mut aws_launcher = aws::Launcher::default();
                aws_launcher.set_mode(aws::LaunchMode::TrySpot { hours: 6 });
                let m = aws::Setup::default()
                    .region_with_ubuntu_ami(region.clone().parse()?)
                    .await?
                    .instance_type("t3.medium")
                    .setup(move |vm| {
                        info!(?vm, "wait");
                        wait_for_continue();
                        let bench_remote_path = bench_remote_path.clone();
                        let bench_bin = bench_bin.clone();
                        let script_remote_path = srp.clone();
                        Box::pin(async move {
                            write_file(&vm.ssh, &bench_bin, bench_remote_path.as_path()).await?;
                            write_file(&vm.ssh, &bench_bin, script_remote_path.as_path()).await?;
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

                let res = with_launcher(&mut aws_launcher, MACHINE_NAME, &script_remote_path).await;
                aws_launcher.terminate_all().await?;
                res
            }
            Node::Azure { region: r } => {
                let srp = script_remote_path.clone();
                let mut az_launcher = azure::Launcher::default();
                let m = azure::Setup::default()
                    .region(r.clone().parse()?)
                    .instance_type("Standard_B2ms".to_owned())
                    .setup(move |vm| {
                        let bench_remote_path = bench_remote_path.clone();
                        let bench_bin = bench_bin.clone();
                        let script_remote_path = srp.clone();
                        Box::pin(async move {
                            write_file(&vm.ssh, &bench_bin, bench_remote_path.as_path()).await?;
                            write_file(&vm.ssh, &bench_bin, script_remote_path.as_path()).await?;
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

                let res = with_launcher(&mut az_launcher, MACHINE_NAME, &script_remote_path).await;
                az_launcher.terminate_all().await?;
                res
            }
            Node::Baremetal { ip: i, user: u } => {
                let mut launcher = baremetal::Machine::default();
                let srp = script_remote_path.clone();
                let m =
                    tsunami::providers::baremetal::Setup::new((i.as_str(), 22), Some(u.clone()))?
                        .setup(move |vm| {
                            let bench_remote_path = bench_remote_path.clone();
                            let bench_bin = bench_bin.clone();
                            let script_remote_path = srp.clone();
                            Box::pin(async move {
                                write_file(&vm.ssh, &bench_bin, bench_remote_path.as_path())
                                    .await?;
                                write_file(&vm.ssh, &bench_bin, script_remote_path.as_path())
                                    .await?;
                                Ok(())
                            })
                        });
                // termination doesn't matter here
                launcher
                    .spawn(vec![(MACHINE_NAME.to_owned(), m)], None)
                    .await?;
                let conns = launcher.connect_all().await?;
                let vm = conns.get(MACHINE_NAME).unwrap();
                do_exp(&vm.ssh, &script_remote_path).await
            }
        }
    }
}

async fn do_exp(ssh: &Session, script_remote_path: &Path) -> Result<(), Report> {
    info!(?ssh, "wait");
    wait_for_continue();

    let mut cmd = ssh.command("python3");
    cmd.arg(script_remote_path.to_str().unwrap());
    info!(?cmd, "running");
    let status = cmd.status().await?;
    ensure!(status.success(), "script failed");
    info!("getting files");
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
