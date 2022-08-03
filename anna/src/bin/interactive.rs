use anna_client_tokio::{config::Config, nodes::client};

fn main() -> eyre::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "{:?}",
            eyre::Error::new(err).wrap_err("failed to set up logger")
        );
    }

    let config: Config = argh::from_env();
    client::run_interactive(
        &config,
        &mut std::io::stdin(),
        &mut std::io::stdout(),
        &mut std::io::stderr(),
        false,
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
        .apply()?;
    Ok(())
}
