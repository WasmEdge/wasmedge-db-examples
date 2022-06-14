use anna::{
    anna_default_zenoh_prefix,
    messages::TcpMessage,
    nodes::{receive_tcp_message, request_cluster_info, send_tcp_message},
    topics::RoutingThread,
    ZenohValueAsString,
};
use argh::FromArgs;
use eyre::{eyre, Context, ContextCompat};
use rand::prelude::SliceRandom;
use smol::net::TcpStream;
use std::{
    collections::BTreeMap,
    iter::Extend,
    net::SocketAddr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use zenoh::prelude::{Receiver, ZFuture};

#[derive(FromArgs)]
/// Rusty anna client
struct Args {
    #[argh(positional)]
    thread_num: u32,

    #[argh(positional)]
    iteration_num: u32,
}

fn main() -> eyre::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "{:?}",
            eyre::Error::new(err).wrap_err("failed to set up logger")
        );
    }

    let args: Args = argh::from_env();

    let zenoh = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .wait()
            .map_err(|e| eyre!(e))?,
    );
    let zenoh_prefix = anna_default_zenoh_prefix();

    let cluster_info = smol::block_on(request_cluster_info(&zenoh, &zenoh_prefix))?;

    let routing_thread = RoutingThread::new(
        cluster_info
            .routing_node_ids
            .choose(&mut rand::thread_rng())
            .cloned()
            .context("no routing nodes detected")?,
        0,
    );
    let routing_node_tcp = {
        let reply = {
            let topic = routing_thread.tcp_addr_topic(&zenoh_prefix);
            let receiver = zenoh
                .get(&topic)
                .wait()
                .map_err(|e| eyre!(e))
                .context("failed to query tcp address of routing thread")?;
            receiver.recv()?
        };
        let parsed: smol::net::SocketAddr = serde_json::from_str(&reply.sample.value.as_string()?)
            .context("failed to deserialize tcp addr reply")?;

        parsed
    };

    let thread_num = args.thread_num;
    let iteration_num = args.iteration_num;

    if thread_num != 0 {
        println!("\ntcp query: ");
        let start = Instant::now();
        let latency = run_tcp(
            thread_num,
            iteration_num,
            zenoh.clone(),
            zenoh_prefix,
            routing_thread.clone(),
            routing_node_tcp,
        );
        println!("  average latency: {:?}", latency);
        println!("  total time: {:?}", Instant::now() - start);
        println!(
            "  throughput: {:?} msg/s",
            (thread_num * iteration_num) as f32 / (Instant::now() - start).as_secs_f32()
        );

        println!("\nzenoh query: ");
        let start = Instant::now();
        let latency = run_zenoh_query(
            thread_num,
            iteration_num,
            zenoh.clone(),
            zenoh_prefix,
            routing_thread.clone(),
            routing_node_tcp,
        );
        println!("  average latency: {:?}", latency);
        println!("  total time: {:?}", Instant::now() - start);
        println!(
            "  throughput: {:?} msg/s",
            (thread_num * iteration_num) as f32 / (Instant::now() - start).as_secs_f32()
        );

        println!("\nzenoh pub/sub: ");
        let start = Instant::now();
        let latency = run_zenoh_pub_sub(
            thread_num,
            iteration_num,
            zenoh,
            zenoh_prefix,
            routing_thread,
            routing_node_tcp,
        );
        println!("  average latency: {:?}", latency);
        println!("  total time: {:?}", Instant::now() - start);
        println!(
            "  throughput: {:?} msg/s",
            (thread_num * iteration_num) as f32 / (Instant::now() - start).as_secs_f32()
        );
    } else {
        println!("Number of Threads | Messages per thread | Zenoh query | Zenoh pub/sub | TCP");
        println!("------------------|---------------------|-------------|---------------|----");

        let mut zenoh_query_results = BTreeMap::new();
        let mut zenoh_pub_sub_results = BTreeMap::new();
        let mut tcp_results = BTreeMap::new();

        for (f, map, name) in [
            (run_tcp as BenchFn, &mut tcp_results, "tcp"),
            (run_zenoh_query, &mut zenoh_query_results, "zenoh query"),
            (
                run_zenoh_pub_sub,
                &mut zenoh_pub_sub_results,
                "zenoh pub/sub",
            ),
        ] {
            eprintln!("{}:", name);
            for thread_num in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] {
                eprintln!("  threads: {}", thread_num);
                for iteration_num in [1, 10, 100, 1000, 10000] {
                    if thread_num >= 64 && iteration_num >= 10000 {
                        continue; // skip
                    }
                    if thread_num >= 512 && iteration_num >= 1000 {
                        continue; // skip
                    }
                    if thread_num >= 1024 && iteration_num >= 100 {
                        continue; // skip
                    }
                    if thread_num >= 2048 && iteration_num >= 10 {
                        continue; // skip
                    }

                    eprint!("    iterations: {}", iteration_num);

                    let latency = f(
                        thread_num,
                        iteration_num,
                        zenoh.clone(),
                        zenoh_prefix,
                        routing_thread.clone(),
                        routing_node_tcp,
                    );
                    eprintln!(" -> {:?}", latency);

                    map.insert((thread_num, iteration_num), latency);
                }
            }
        }

        for key in tcp_results.keys() {
            let (thread_num, iteration_num) = key;
            println!(
                "{} | {} | {:?} | {:?} | {:?}",
                thread_num,
                iteration_num,
                zenoh_query_results[key],
                zenoh_pub_sub_results[key],
                tcp_results[key],
            );
        }
    }

    Ok(())
}

type BenchFn = for<'r> fn(
    u32,
    u32,
    Arc<zenoh::Session>,
    &'r str,
    RoutingThread,
    std::net::SocketAddr,
) -> std::time::Duration;

fn run_tcp(
    thread_num: u32,
    iteration_num: u32,
    _zenoh: Arc<zenoh::Session>,
    _zenoh_prefix: &str,
    _routing_thread: RoutingThread,
    routing_node_tcp: SocketAddr,
) -> Duration {
    let mut threads = Vec::new();
    for _ in 0..thread_num {
        threads.push(thread::spawn(move || {
            let start = Instant::now();
            let new_connection = || {
                let connection = smol::block_on(TcpStream::connect(routing_node_tcp))
                    .expect("failed to connect to tcp stream");
                connection
                    .set_nodelay(true)
                    .expect("failed to set nodelay for tcpstream");
                connection
            };
            let mut connection = new_connection();
            let mut latency_sum = Instant::now() - start;
            for _ in 0..iteration_num {
                let start = Instant::now();

                let payload = vec![1, 2, 3, 4];
                let ping = TcpMessage::Ping {
                    payload: payload.clone(),
                };
                smol::block_on(async {
                    let reply = loop {
                        send_tcp_message(&ping, &mut connection).await.unwrap();
                        if let Some(reply) = receive_tcp_message(&mut connection).await.unwrap() {
                            break reply;
                        }
                        // connection was closed -> reconnect
                        connection = new_connection();
                    };
                    assert_eq!(reply, TcpMessage::Pong { payload });
                });

                latency_sum += Instant::now() - start;
            }

            connection.shutdown(smol::net::Shutdown::Both).unwrap();

            latency_sum / iteration_num
        }));
    }
    let latency_sum: Duration = threads.into_iter().map(|t| t.join().unwrap()).sum();
    let latency_avg = latency_sum / thread_num;
    latency_avg
}

fn run_zenoh_query(
    thread_num: u32,
    iteration_num: u32,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: &str,
    routing_thread: RoutingThread,
    _routing_node_tcp: SocketAddr,
) -> Duration {
    let zenoh_prefix = Arc::new(zenoh_prefix.to_owned());

    let mut threads = Vec::new();
    for _ in 0..thread_num {
        let routing_thread = routing_thread.clone();
        let zenoh_prefix = zenoh_prefix.clone();
        let zenoh = zenoh.clone();
        threads.push(thread::spawn(move || {
            let mut latency_sum = Duration::default();
            for _ in 0..iteration_num {
                let start = Instant::now();

                let reply = zenoh
                    .get(&routing_thread.ping_topic(&zenoh_prefix))
                    .wait()
                    .unwrap()
                    .recv()
                    .unwrap()
                    .sample
                    .value
                    .as_string()
                    .unwrap();
                assert_eq!(reply.as_str(), "pong");

                latency_sum += Instant::now() - start;
            }
            latency_sum / iteration_num
        }));
    }
    let latency_sum: Duration = threads.into_iter().map(|t| t.join().unwrap()).sum();
    let latency_avg = latency_sum / thread_num;
    latency_avg
}

fn run_zenoh_pub_sub(
    thread_num: u32,
    iteration_num: u32,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: &str,
    routing_thread: RoutingThread,
    _routing_node_tcp: SocketAddr,
) -> Duration {
    let zenoh_prefix = Arc::new(zenoh_prefix.to_owned());
    let mut threads = Vec::new();
    for _ in 0..thread_num {
        let zenoh = zenoh.clone();
        let routing_thread = routing_thread.clone();
        let zenoh_prefix = zenoh_prefix.clone();

        threads.push(thread::spawn(move || {
            let start = Instant::now();
            let reply_topic = format!("{}/{}", zenoh_prefix, uuid::Uuid::new_v4().to_string());
            let mut reply_subscriber = zenoh.subscribe(reply_topic.as_str()).wait().unwrap();
            let mut latency_sum = Instant::now() - start;
            for _ in 0..iteration_num {
                let start = Instant::now();
                zenoh
                    .put(
                        &routing_thread.ping_topic(&zenoh_prefix),
                        reply_topic.as_str(),
                    )
                    .wait()
                    .unwrap();
                let reply = reply_subscriber
                    .receiver()
                    .recv()
                    .unwrap()
                    .value
                    .as_string()
                    .unwrap();
                assert_eq!(reply.as_str(), "pong");
                latency_sum += Instant::now() - start;
            }
            latency_sum / iteration_num
        }));
    }
    let latency_sum: Duration = threads.into_iter().map(|t| t.join().unwrap()).sum();
    let latency_avg = latency_sum / thread_num;
    latency_avg
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
        .chain(fern::log_file("ping.log")?)
        .apply()?;
    Ok(())
}
