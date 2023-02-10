use redis::Commands;
use anyhow::Result;

fn get_url() -> String {
    if let Ok(url) = std::env::var("REDIS_URL") {
        url
    } else {
        "redis://127.0.0.1/".into()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // connect to redis
    let client = redis::Client::open(&*get_url())?;
    let mut con = client.get_connection()?;

    let time = format!("{}", chrono::Utc::now());
    // throw away the result, just make sure it does not fail
    let _ : () = con.set("current_time", time)?;

    // read back the key and return it.  Because the return value
    // from the function is a result for String, this will automatically
    // convert into one.
    let value : String = con.get("current_time")?;
    println!("Successfully GET `time`: {}", value);

    Ok(())
}
