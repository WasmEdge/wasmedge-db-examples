use mysql_async::{prelude::*, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Result};
use time::PrimitiveDateTime;

fn get_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:pass@127.0.0.1:3306/mysql".into()
    }
}

#[derive(Debug)]
struct CpuMetric {
    hostname: String,
    environment: String,
    usage_user: f64,
    usage_system: f64,
    usage_idle: f64,
    ts: i64,
}

impl CpuMetric {
    fn new(
        hostname: String,
        environment: String,
        usage_user: f64,
        usage_system: f64,
        usage_idle: f64,
        ts: i64,
    ) -> Self {
        Self {
            hostname,
            environment,
            usage_user,
            usage_system,
            usage_idle,
            ts,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Alternative: The "easy" way with a default connection pool
    // let pool = Pool::new(Opts::from_url(&*get_url()).unwrap());
    // let mut conn = pool.get_conn().await.unwrap();

    // Below we create a customized connection pool
    let opts = Opts::from_url(&*get_url()).unwrap();
    let builder = OptsBuilder::from_opts(opts);
    // Disabled after we figured out tls issue.
    // if std::env::var("DATABASE_SSL").is_ok() {
    //     let ssl_opts = SslOpts::default()
    //         .with_danger_accept_invalid_certs(true)
    //         .with_danger_skip_domain_validation(true);
    //     builder = builder.ssl_opts(ssl_opts);
    // }
    // The connection pool will have a min of 5 and max of 10 connections.
    let constraints = PoolConstraints::new(1, 2).unwrap();
    let pool_opts = PoolOpts::default().with_constraints(constraints);

    let pool = Pool::new(builder.pool_opts(pool_opts));
    let mut conn = pool.get_conn().await.unwrap();
    // Create table if not exists
    r"CREATE TABLE IF NOT EXISTS wasmedge_example_cpu_metrics (
    hostname STRING,
    environment STRING,
    usage_user DOUBLE,
    usage_system DOUBLE,
    usage_idle DOUBLE,
    ts TIMESTAMP,
    TIME INDEX(ts),
    PRIMARY KEY(hostname, environment)
);"
    .ignore(&mut conn)
    .await?;
    println!("Table created!");

    println!("Ingest some data...");
    let metrics = vec![
        CpuMetric::new(
            "host0".into(),
            "test".into(),
            32f64,
            3f64,
            4f64,
            1680307200050,
        ),
        CpuMetric::new(
            "host1".into(),
            "test".into(),
            29f64,
            32f64,
            50f64,
            1680307200050,
        ),
        CpuMetric::new(
            "host0".into(),
            "test".into(),
            32f64,
            3f64,
            4f64,
            1680307260050,
        ),
        CpuMetric::new(
            "host1".into(),
            "test".into(),
            29f64,
            32f64,
            50f64,
            1680307260050,
        ),
        CpuMetric::new(
            "host0".into(),
            "test".into(),
            32f64,
            3f64,
            4f64,
            1680307320050,
        ),
        CpuMetric::new(
            "host1".into(),
            "test".into(),
            29f64,
            32f64,
            50f64,
            1680307320050,
        ),
    ];

    r"INSERT INTO wasmedge_example_cpu_metrics (hostname, environment, usage_user, usage_system, usage_idle, ts)
      VALUES (:hostname, :environment, :usage_user, :usage_system, :usage_idle, :ts)"
        .with(metrics.iter().map(|metric| {
            params! {
                "hostname" => &metric.hostname,
                "environment" => &metric.environment,
                "usage_user" => metric.usage_user,
                "usage_system" => metric.usage_system,
                "usage_idle" => metric.usage_idle,
                "ts" => metric.ts,
            }
        }))
        .batch(&mut conn)
        .await?;

    // query data
    println!("Query some data");
    let loaded_metrics = "SELECT * FROM wasmedge_example_cpu_metrics"
        .with(())
        .map(
            &mut conn,
            |(hostname, environment, usage_user, usage_system, usage_idle, raw_ts): (
                String,
                String,
                f64,
                f64,
                f64,
                PrimitiveDateTime,
            )| {
                let ts = raw_ts.assume_utc().unix_timestamp() * 1000;
                CpuMetric::new(
                    hostname,
                    environment,
                    usage_user,
                    usage_system,
                    usage_idle,
                    ts,
                )
            },
        )
        .await?;
    println!("{:?}", loaded_metrics);

    // Dropped connection will go to the pool
    drop(conn);
    // The Pool must be disconnected explicitly because
    // it's an asynchronous operation.
    pool.disconnect().await?;
    Ok(())
}
