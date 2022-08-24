# Examples of WasmEdge Anna Client

[**wasmedge-anna-client**](https://github.com/WasmEdge/wasmedge-anna-client) is a Rust client for [anna-rs] based on [Tokio for WasmEdge](https://github.com/WasmEdge/tokio/tree/wasmedge). It communicates with Anna routing nodes and KVS nodes via vanilla TCP connections instead of Zenoh.

[anna-rs]: https://github.com/essa-project/anna-rs

## Run the example

First, run routing node and KVS node of [anna-rs]:

```sh
$ git clone https://github.com/essa-project/anna-rs.git
$ cd anna-rs
$ cp example-config.yml config.yml
$ ANNA_PUBLIC_IP=127.0.0.1 ANNA_TCP_PORT_BASE=12340 cargo run --bin routing -- config.yml
$ # in another shell
$ ANNA_PUBLIC_IP=127.0.0.1 cargo run --bin kvs -- config.yml
```

Then, build and run the example app in this repo:

```sh
$ git clone https://github.com/WasmEdge/wasmedge-db-examples.git
$ cd wasmedge-db-examples/anna
$ cargo build --target wasm32-wasi
$ /path/to/wasmedge --dir .:. target/wasm32-wasi/debug/putget.wasm
```
