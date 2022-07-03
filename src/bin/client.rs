use anna::{anna_default_zenoh_prefix, config::Config, nodes::client};
use argh::FromArgs;
use eyre::Context;
use std::{
    fs::{self, File},
    io::Read,
    iter::Extend,
    path::PathBuf,
};

#[derive(FromArgs)]
/// Rusty anna client
struct Args {
    #[argh(positional)]
    config_file: PathBuf,

    #[argh(positional)]
    input_file: Option<PathBuf>,
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

    let zenoh_prefix = anna_default_zenoh_prefix();

    let mut input = args
        .input_file
        .map(|path| File::open(&path).context("failed to open input file"))
        .transpose()?
        .map(|r| {
            let boxed: Box<dyn Read> = Box::new(r);
            boxed
        })
        .unwrap_or_else(|| Box::new(std::io::stdin()));

    client::run_interactive(
        &config,
        &mut input,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
        false,
        zenoh_prefix.to_owned(),
    )
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
        .chain(fern::log_file("client.log")?)
        .apply()?;
    Ok(())
}
