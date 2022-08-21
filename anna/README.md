# wasmedge-anna-driver

**wasmedge-anna-driver** is a Rust client for [anna-rs] based on [Tokio for WasmEdge](https://github.com/WasmEdge/tokio/tree/wasmedge). It communicates with Anna routing nodes and KVS nodes via vanilla TCP connections instead of Zenoh.

[anna-rs]: https://github.com/essa-project/anna-rs

## Usage

```rust
use std::time::Duration;
use wasmedge_anna_driver::{Client, ClientConfig};

let mut client = Client::new(ClientConfig {
    routing_ip: "127.0.0.1".parse().unwrap(),
    routing_port_base: 12340,
    routing_threads: 1,
    timeout: Duration::from_secs(10),
})?;

// put the value
client.put_lww("foo".into(), "bar".into()).await?;

// sleep 1 second
tokio::time::sleep(Duration::from_secs(1)).await;

// get the value
let bytes = client.get_lww("foo".into()).await?;
let value = String::from_utf8(bytes)?;
println!("Successfully GET value of `foo`: {}", value);
```

## Run the example

First, run routing node and KVS node of [anna-rs]:

```sh
$ cd anna-rs
$ cp example-config.yml config.yml
$ ANNA_PUBLIC_IP=127.0.0.1 ANNA_TCP_PORT_BASE=12340 cargo run --bin routing -- config.yml
$ # in another shell
$ ANNA_PUBLIC_IP=127.0.0.1 cargo run --bin kvs -- config.yml
```

Then, build and run the example app of **wasmedge-anna-driver**:

```sh
$ cd wasmedge-db-drivers/anna/examples/simple-put-get
$ cargo build --target wasm32-wasi
$ /path/to/wasmedge --dir .:. target/wasm32-wasi/debug/simple-put-get.wasm
```

## Attribution

Many code of this driver is derived from [anna-rs].
