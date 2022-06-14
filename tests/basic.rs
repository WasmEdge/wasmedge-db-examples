use anna::{
    config::{Capacities, Config, Policy, Replication, Threads},
    nodes::{client, kvs, routing},
    zenoh_test_instance,
};
use eyre::Context;
use pretty_assertions::assert_eq;
use std::{io::Cursor, sync::Arc, thread};

#[test]
fn get_set() {
    let _ = set_up_logger();

    let config = Arc::new(Config {
        threads: Threads {
            memory: 1,
            ebs: 1,
            routing: 1,
            benchmark: 1,
        },
        replication: Replication {
            memory: 1,
            ebs: 0,
            local: 1,
            minimum: 1,
        },
        capacities: Capacities {
            memory_cap: 1,
            ebs_cap: 0,
        },
        policy: Policy {
            elasticity: false,
            tiering: false,
            selective_rep: false,
        },
    });

    let zenoh = zenoh_test_instance();
    let zenoh_prefix = uuid::Uuid::new_v4().to_string();

    let _router_thread = {
        let config = config.clone();
        let zenoh = zenoh.clone();
        let zenoh_prefix = zenoh_prefix.clone();
        thread::spawn(move || {
            routing::run(&config, zenoh, zenoh_prefix, None)
                .context("Routing task failed")
                .unwrap()
        })
    };

    let _kvs_thread = {
        let config = config.clone();
        let zenoh = zenoh.clone();
        let zenoh_prefix = zenoh_prefix.clone();
        thread::spawn(move || {
            kvs::run(&config, zenoh, zenoh_prefix, None)
                .context("Kvs task failed")
                .unwrap()
        })
    };

    let input = "\
        PUT a 1\n\
        GET a\n\
        PUT b 2\n\
        GET b\n\
        PUT a 10 \n\
        GET a\n\
        PUT_SET set 1 2 3 \n\
        GET_SET set\n\
        PUT_SET set 1 2 4\n\
        GET_SET set\n\
        PUT_CAUSAL c hello\n\
        GET_CAUSAL c\n\
    ";
    let mut stdin = input.as_bytes();
    let mut stdout = Cursor::new(Vec::new());
    let mut stderr = Cursor::new(Vec::new());

    client::run_interactive(
        &config,
        &mut stdin,
        &mut stdout,
        &mut stderr,
        true,
        zenoh.clone(),
        zenoh_prefix.clone(),
    )
    .unwrap();
    assert_eq!(String::from_utf8(stderr.into_inner()).unwrap(), "");
    assert_eq!(
        String::from_utf8(stdout.into_inner())
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        "kvs> Success!\n\
        kvs> 1\n\
        kvs> Success!\n\
        kvs> 2\n\
        kvs> Success!\n\
        kvs> 10\n\
        kvs> Success!\n\
        kvs> { 1 2 3 }\n\
        kvs> Success!\n\
        kvs> { 1 2 3 4 }\n\
        kvs> Success!\n\
        kvs> {test : 1}\n\
        dep1 : {test1 : 1}\n\
        hello\n\
        kvs> "
            .lines()
            .collect::<Vec<_>>()
    );
}

fn set_up_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}] {}",
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
