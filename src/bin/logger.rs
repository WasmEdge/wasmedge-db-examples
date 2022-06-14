use zenoh::prelude::{SplitBuffer, ZFuture};

fn main() {
    let zenoh = zenoh::open(zenoh::config::Config::default())
        .wait()
        .unwrap();

    let topic = std::env::args().skip(1).next().unwrap_or("/**".into());
    let mut sub = zenoh.subscribe(topic).wait().unwrap();

    for sample in sub.receiver().iter() {
        let value = match String::from_utf8(sample.value.payload.contiguous().into_owned()) {
            Err(_) => "<invalid UTF8>".to_string(),
            Ok(v) => v.to_string(),
        };

        let value_shortened = if value.len() > 500 {
            let index = (0..500)
                .rev()
                .filter(|&i| value.is_char_boundary(i))
                .next()
                .unwrap();
            &value[..index]
        } else {
            &value[..]
        };

        println!(
            ">>>>> [{}] {}\n{}\n",
            sample
                .timestamp
                .map(|t| t.get_time().to_string())
                .unwrap_or_else(|| "<unknown>".to_owned()),
            sample.key_expr,
            value_shortened
        );
    }
}
