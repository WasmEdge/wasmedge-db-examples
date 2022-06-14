use anna::{
    anna_default_zenoh_prefix,
    config::Config,
    lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice},
    messages::user_feedback::{KeyLatency, UserFeedback},
    nodes::{client::ClientNode, request_cluster_info},
    store::LatticeValue,
    topics::{benchmark_topic, MonitoringThread, RoutingThread},
    ClientKey, ZenohValueAsString,
};
use argh::FromArgs;
use eyre::{anyhow, bail, Context};
use std::{
    collections::HashMap,
    convert::TryInto,
    fs,
    iter::Extend,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use zenoh::prelude::ZFuture;

#[derive(FromArgs)]
/// Rusty anna client
struct Args {
    #[argh(positional)]
    config_file: PathBuf,
}

fn main() -> eyre::Result<()> {
    let args: Args = argh::from_env();

    let config: Config = serde_yaml::from_str(
        &fs::read_to_string(&args.config_file).context("failed to read config file")?,
    )
    .context("failed to parse config file")?;

    let thread_num = config.threads.benchmark;

    for (thread_id, thread) in (0..thread_num)
        .map(|thread_id| {
            let config = config.clone();
            thread::spawn(move || run(thread_id, config))
        })
        .enumerate()
    {
        match thread.join() {
            Ok(result) => result.with_context(|| format!("thread {} failed", thread_id))?,
            Err(panic_payload) => std::panic::resume_unwind(panic_payload),
        }
    }

    Ok(())
}

fn set_up_logger(thread_id: u32) -> Result<(), fern::InitError> {
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
        .chain(fern::log_file(format!(
            "benchmark-thread-{}.log",
            thread_id
        ))?)
        .apply()?;
    Ok(())
}

fn run(thread_id: u32, config: Config) -> eyre::Result<()> {
    if let Err(err) = set_up_logger(thread_id) {
        eprintln!(
            "{:?}",
            eyre::Error::new(err).wrap_err("failed to set up logger")
        );
    }

    let zenoh = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .wait()
            .map_err(|e| eyre::eyre!(e))?,
    );
    let zenoh_prefix = anna_default_zenoh_prefix();

    let node_id = format!("bench-client-{}", uuid::Uuid::new_v4());

    let timeout = Duration::from_secs(10);

    let cluster_info = smol::block_on(request_cluster_info(&zenoh, &zenoh_prefix))?;
    let routing_ips = cluster_info.routing_node_ids.iter(); // TODO: also consider config.user.routing-elb
    let routing_threads: Vec<_> = routing_ips
        .flat_map(|id| (0..config.threads.routing).map(move |i| RoutingThread::new(id.clone(), i)))
        .collect();

    let mut client = ClientNode::new(
        node_id.clone(),
        thread_id,
        routing_threads,
        timeout,
        zenoh.clone(),
        zenoh_prefix.to_owned(),
    )?;
    smol::block_on(client.init_tcp_connections())?;

    let mut commands = zenoh
        .subscribe(&benchmark_topic(thread_id, zenoh_prefix))
        .wait()
        .map_err(|e| eyre::eyre!(e))
        .context("failed to declare subscriber for benchmark commands")?;

    for message in commands.receiver().iter() {
        let serialized = message.value.as_string()?;
        log::info!("Received benchmark command `{}`", serialized);

        let mut split = serialized.split(':');
        let mode = split.next().ok_or_else(|| anyhow!("command was empty"))?;
        match mode {
            "CACHE" => {
                let num_keys = split
                    .next()
                    .ok_or_else(|| anyhow!("cache command does not contain `:` separator"))?
                    .parse()
                    .context("failed to parse number of keys")?;

                // warm up cache
                client.clear_cache();
                let warmup_start = Instant::now();

                for i in 1..=num_keys {
                    if i % 50000 == 0 {
                        log::info!("Warming up cache for key {}", i);
                    }
                    smol::block_on(client.get_async(generate_key(i)))?;
                    receive(&mut client)?;
                }

                let warmup_time = Instant::now() - warmup_start;
                log::info!("Cache warmup took {:.2} seconds", warmup_time.as_secs_f32());
            }
            "LOAD" => {
                let ty = split
                    .next()
                    .ok_or_else(|| anyhow!("load command does not contain `:` separator"))?;
                let num_keys = split
                    .next()
                    .ok_or_else(|| anyhow!("missing num_keys parameter"))?
                    .parse()
                    .context("failed to parse number of keys")?;
                let len: usize = split
                    .next()
                    .ok_or_else(|| anyhow!("missing len parameter"))?
                    .parse()
                    .context("failed to parse length")?;
                let report_period = Duration::from_secs(
                    split
                        .next()
                        .ok_or_else(|| anyhow!("missing report_period parameter"))?
                        .parse()
                        .context("failed to parse report period")?,
                );
                let time = Duration::from_secs(
                    split
                        .next()
                        .ok_or_else(|| anyhow!("missing time parameter"))?
                        .parse()
                        .context("failed to parse time")?,
                );
                let zipf = split
                    .next()
                    .ok_or_else(|| anyhow!("missing zipf parameter"))?
                    .parse()
                    .context("failed to parse zipf")?;

                let mut sum_probs = HashMap::new();
                let base;

                if zipf > 0.0 {
                    log::info!("Zipf coefficient is {}.", zipf);
                    base = get_base(num_keys, zipf);

                    sum_probs.insert(0, 0.0);

                    for i in 1..=num_keys {
                        sum_probs.insert(i, sum_probs[&(i - 1)] + base / (i as f64).powf(zipf));
                    }
                } else {
                    log::info!("Using a uniform random distribution");
                }

                let mut count = 0;
                let benchmark_start = Instant::now();
                let mut benchmark_end;
                let mut epoch_start = Instant::now();
                let mut epoch_end;
                let mut total_time;

                let mut epoch = 1;
                let mut observed_latency: HashMap<ClientKey, (Duration, u32)> = HashMap::new();

                loop {
                    let k = if zipf > 0.0 {
                        sample(num_keys, &sum_probs)
                    } else {
                        rand::random::<usize>() % num_keys + 1
                    };

                    let key = generate_key(k);

                    match ty {
                        "G" => {
                            smol::block_on(client.get_async(key))?;
                            receive(&mut client)?;
                            count += 1;
                        }
                        "P" => {
                            let ts = Timestamp::now();
                            let val = LastWriterWinsLattice::from_pair(
                                ts,
                                (0..len).map(|_| b'a').collect::<Vec<_>>(),
                            );
                            smol::block_on(client.put_async(key, LatticeValue::Lww(val)))?;
                            receive(&mut client)?;
                            count += 1;
                        }
                        "M" => {
                            let req_start = Instant::now();
                            let ts = Timestamp::now();
                            let val = LastWriterWinsLattice::from_pair(
                                ts,
                                (0..len).map(|_| b'a').collect::<Vec<_>>(),
                            );

                            smol::block_on(client.put_async(key.clone(), LatticeValue::Lww(val)))?;
                            receive(&mut client)?;
                            smol::block_on(client.get_async(key.clone()))?;
                            receive(&mut client)?;

                            count += 2;

                            let req_end = Instant::now();

                            let key_latency = (req_end - req_start) / 2;

                            match observed_latency.entry(key.clone()) {
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    entry.insert((key_latency, 1));
                                }
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    let (mean_latency, count) = entry.get_mut();
                                    *mean_latency =
                                        ((*mean_latency) * (*count) + key_latency) / (*count + 1);
                                    *count += 1;
                                }
                            }
                        }
                        other => bail!("invalid request type `{}`", other),
                    }

                    epoch_end = Instant::now();
                    let time_elapsed = epoch_end - epoch_start;

                    // report throughput every report_period seconds
                    if time_elapsed >= report_period {
                        let throughput = (count as f64) / time_elapsed.as_secs_f64();
                        log::info!(
                            "[Epoch {}] Throughput is {} ops/seconds.",
                            epoch,
                            throughput
                        );
                        epoch += 1;

                        let latency = 1000000.0 / throughput;

                        let mut feedback = UserFeedback {
                            uid: format!("{}:{}", node_id, thread_id),
                            latency,
                            throughput,
                            finish: Default::default(),
                            warmup: Default::default(),
                            key_latency: Default::default(),
                        };

                        for (key, (key_latency, _)) in &observed_latency {
                            if *key_latency > Duration::from_micros(1) {
                                let kl = KeyLatency {
                                    key: key.to_owned(),
                                    latency: key_latency.as_secs_f64() * 1000. * 1000.,
                                };
                                feedback.key_latency.push(kl);
                            }
                        }

                        let serialized_latency = serde_json::to_string(&feedback)
                            .context("failed to serialize UserFeedback")?;

                        zenoh
                            .put(
                                &MonitoringThread::feedback_report_topic(zenoh_prefix),
                                serialized_latency.as_str(),
                            )
                            .wait()
                            .map_err(|e| eyre::eyre!(e))?;

                        count = 0;
                        observed_latency.clear();
                        epoch_start = Instant::now();
                    }

                    benchmark_end = Instant::now();
                    total_time = benchmark_end - benchmark_start;
                    if total_time > time {
                        break;
                    }
                }
                log::info!("Finished");

                let feedback = UserFeedback {
                    uid: format!("{}:{}", node_id, thread_id),
                    finish: true,
                    latency: Default::default(),
                    throughput: Default::default(),
                    warmup: Default::default(),
                    key_latency: Default::default(),
                };

                let serialized_latency =
                    serde_json::to_string(&feedback).context("failed to serialize UserFeedback")?;

                zenoh
                    .put(
                        &MonitoringThread::feedback_report_topic(zenoh_prefix),
                        serialized_latency.as_str(),
                    )
                    .wait()
                    .map_err(|e| eyre::eyre!(e))?;
            }
            "WARM" => {
                let num_keys: u32 = split
                    .next()
                    .ok_or_else(|| anyhow!("missing num_keys parameter"))?
                    .parse()
                    .context("failed to parse number of keys")?;
                let len: u32 = split
                    .next()
                    .ok_or_else(|| anyhow!("missing len parameter"))?
                    .parse()
                    .context("failed to parse length")?;
                let total_threads: u32 = split
                    .next()
                    .ok_or_else(|| anyhow!("missing total_threads parameter"))?
                    .parse()
                    .context("failed to parse total_threads")?;

                let range = num_keys / total_threads;
                let start = thread_id * range + 1;
                let end = thread_id * range + 1 + range;

                let warmup_start = Instant::now();

                for i in start..end {
                    if i % 50000 == 0 {
                        log::info!("Creating key {}.", i);
                    }

                    let ts = Timestamp::now();
                    let val = LastWriterWinsLattice::from_pair(
                        ts,
                        (0..len).map(|_| b'a').collect::<Vec<_>>(),
                    );

                    smol::block_on(
                        client
                            .put_async(generate_key(i.try_into().unwrap()), LatticeValue::Lww(val)),
                    )?;
                    receive(&mut client)?;
                }

                let warmup_time = Instant::now() - warmup_start;
                log::info!("Warming up data took {:?} seconds.", warmup_time);
            }
            other => bail!("invalid benchmark command `{}`", other),
        }
    }

    Ok(())
}

fn get_base(n: usize, skew: f64) -> f64 {
    let mut base = 0.0;
    for k in 1..=n {
        base += (k as f64).powf(-1.0 * skew);
    }
    1.0 / base
}

fn receive(client: &mut ClientNode) -> eyre::Result<()> {
    smol::block_on(async {
        let mut responses = client.receive_async().await?;
        while responses.is_empty() {
            responses = client.receive_async().await?;
        }
        Ok(())
    })
}

fn sample(n: usize, sum_probs: &HashMap<usize, f64>) -> usize {
    let mut z; // Uniform random number (0 < z < 1)
    let mut zipf_value = 0; // Computed exponential value to be returned
    let mut low;
    let mut high;
    let mut mid; // Binary-search bounds

    // Pull a uniform random number (0 < z < 1)
    loop {
        z = rand::random();
        if !(z == 0.0) {
            break;
        }
    }

    // Map z to the value
    low = 1;
    high = n;

    loop {
        mid = ((low + high) as f64 / 2.).floor() as usize;
        if sum_probs[&mid] >= z && sum_probs[&(mid - 1)] < z {
            zipf_value = mid;
            break;
        } else if sum_probs[&mid] >= z {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
        if low > high {
            break;
        }
    }

    // Assert that zipf_value is between 1 and N
    assert!((zipf_value >= 1) && (zipf_value <= n));

    zipf_value
}

fn generate_key(n: usize) -> ClientKey {
    format!("{:08}", n).into()
}
