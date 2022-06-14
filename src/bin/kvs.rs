use anna::{anna_default_zenoh_prefix, config::Config, nodes::kvs};
use argh::FromArgs;
use eyre::Context;
use std::{fs, path::PathBuf, sync::Arc};
use zenoh::prelude::ZFuture;

#[derive(FromArgs)]
/// Rusty anna node
struct Args {
    #[argh(positional)]
    config_file: PathBuf,
}

fn main() -> eyre::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "{:?}",
            eyre::Error::new(err).wrap_err("failed to set up logger")
        );
    }

    let args: Args = argh::from_env();

    let config: Config = serde_yaml::from_str(
        &fs::read_to_string(&args.config_file).context("failed to read config file")?,
    )
    .context("failed to parse config file")?;

    let zenoh = zenoh::open(zenoh::config::Config::default())
        .wait()
        .map_err(|e| eyre::eyre!(e))?;
    let zenoh_prefix = anna_default_zenoh_prefix();

    let public_ip = std::env::var("ANNA_PUBLIC_IP")
        .ok()
        .map(|ip| ip.parse())
        .transpose()
        .context("failed to parse ANNA_PUBLIC_IP")?;

    kvs::run(&config, Arc::new(zenoh), zenoh_prefix.to_owned(), public_ip)
}

fn set_up_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file("kvs.log")?)
        .apply()?;
    Ok(())
}
