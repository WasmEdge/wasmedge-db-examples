use std::time::Duration;

use wasmedge_anna_driver::{Client, ClientConfig};

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
        .level(log::LevelFilter::Trace)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> eyre::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "{:?}",
            eyre::Error::new(err).wrap_err("failed to set up logger")
        );
    }

    let mut client = Client::new(ClientConfig {
        routing_ip: "127.0.0.1".parse().unwrap(),
        routing_port_base: 12340,
        routing_threads: 1,
        timeout: Duration::from_secs(10),
    })?;

    let time = format!("{}", chrono::Utc::now());
    client.put_lww("time".into(), time.into()).await?; // put the value
    println!("Successfully PUT `time`");
    tokio::time::sleep(Duration::from_secs(1)).await; // sleep 1 second
    let bytes = client.get_lww("time".into()).await?;
    let value = String::from_utf8(bytes)?;
    println!("Successfully GET `time`: {}", value);

    Ok(())
}
