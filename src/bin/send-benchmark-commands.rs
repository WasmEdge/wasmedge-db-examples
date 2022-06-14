use anna::{anna_default_zenoh_prefix, topics::benchmark_topic};
use std::{fmt::Display, str::FromStr};
use zenoh::prelude::ZFuture;

/// Sends a benchmark command to a benchmark node.
#[derive(argh::FromArgs)]
struct Args {
    /// benchmark thread that the command should be sent to.
    #[argh(option, default = "0")]
    thread_id: u32,

    #[argh(subcommand)]
    command: Command,
}

#[derive(argh::FromArgs)]
#[argh(subcommand)]
enum Command {
    Cache(CacheCommand),
    Load(LoadCommand),
    Warm(WarmCommand),
    Exit(ExitCommand),
}

/// Sends a cache command.
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "cache")]
struct CacheCommand {
    #[argh(positional)]
    num_keys: usize,
}

/// Sends a load command.
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "load")]
struct LoadCommand {
    #[argh(positional)]
    ty: LoadType,
    #[argh(positional)]
    num_keys: usize,
    #[argh(positional)]
    len: usize,
    #[argh(positional)]
    report_period_secs: u64,
    #[argh(positional)]
    time_secs: u64,
    #[argh(positional, default = "0.0")]
    zipf: f64,
}

enum LoadType {
    G,
    P,
    M,
}

impl FromStr for LoadType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "G" => Ok(Self::G),
            "P" => Ok(Self::P),
            "M" => Ok(Self::M),
            _ => Err("invalid load type"),
        }
    }
}

impl Display for LoadType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            LoadType::G => f.write_str("G"),
            LoadType::P => f.write_str("P"),
            LoadType::M => f.write_str("M"),
        }
    }
}

/// Sends a warm command.
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "warm")]
struct WarmCommand {
    #[argh(positional)]
    num_keys: u32,
    #[argh(positional)]
    len: u32,
    #[argh(positional)]
    total_threads: u32,
}

/// Exits the benchmark runner.
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "exit")]
struct ExitCommand {}

fn main() -> eyre::Result<()> {
    let args: Args = argh::from_env();

    let zenoh = zenoh::open(zenoh::config::Config::default())
        .wait()
        .map_err(|e| eyre::eyre!(e))?;
    let zenoh_prefix = anna_default_zenoh_prefix();

    match args.command {
        Command::Cache(command) => {
            let serialized = format!("CACHE:{}", command.num_keys);
            smol::block_on(zenoh.put(&benchmark_topic(args.thread_id, zenoh_prefix), serialized))
                .map_err(|e| eyre::eyre!(e))?;
        }
        Command::Load(c) => {
            let serialized = format!(
                "LOAD:{}:{}:{}:{}:{}:{}",
                c.ty, c.num_keys, c.len, c.report_period_secs, c.time_secs, c.zipf
            );
            smol::block_on(zenoh.put(&benchmark_topic(args.thread_id, zenoh_prefix), serialized))
                .map_err(|e| eyre::eyre!(e))?;
        }
        Command::Warm(c) => {
            let serialized = format!("WARM:{}:{}:{}", c.num_keys, c.len, c.total_threads);
            smol::block_on(zenoh.put(&benchmark_topic(args.thread_id, zenoh_prefix), serialized))
                .map_err(|e| eyre::eyre!(e))?;
        }
        Command::Exit(ExitCommand {}) => {
            let serialized = format!("EXIT");
            smol::block_on(zenoh.put(&benchmark_topic(args.thread_id, zenoh_prefix), serialized))
                .map_err(|e| eyre::eyre!(e))?;
        }
    }

    Ok(())
}
