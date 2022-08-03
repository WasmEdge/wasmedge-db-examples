# wasmedge-anna-driver

**wasmedge-anna-driver** is a Rust client for [anna-rs](https://github.com/essa-project/anna-rs) based on [Tokio for WasmEdge](https://github.com/WasmEdge/tokio/tree/wasmedge).

## Usage

First, run routing node and KVS node of [the adapted version of anna-rs](https://github.com/second-state/anna-rs):

```sh
$ cd anna-rs
$ cp example-config.yml config.yml
$ ANNA_PUBLIC_IP=127.0.0.1 ANNA_TCP_PORT_BASE=12340 cargo run --bin routing -- config.yml
$ # in another shell instance
$ ANNA_PUBLIC_IP=127.0.0.1 cargo run --bin kvs -- config.yml
```

Then, build and run the test app of **wasmedge-anna-driver**:

```sh
$ cd wasmedge-db-drivers/anna
$ cargo build --target wasm32-wasi
$ /path/to/wasmedge --dir .:. target/wasm32-wasi/debug/testng.wasm -h 127.0.0.1 -p 12340 -r 2
# -h: IP address of routing node
# -p: Base TCP port of routing threads
# -r: Number of routinge threads
```
