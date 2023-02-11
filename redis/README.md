# Examples for WasmEdge Redis Client

## Linux CLI usage

Prerequisites:

* Install the Rust compiler and Cargo tools
* Install the WasmEdge Runtime
* Install and start the Redis server 

The following commands build and run the example.

```bash
cargo build --target wasm32-wasi --release
wasmedge --env "REDIS_URL=redis://localhost/" target/wasm32-wasi/release/wasmedge-redis-client-examples.wasm
```

## Docker usage

The following command builds and starts two containers. All you need is Docker Desktop. There is no need to install Rust or Redis or even WasmEdge.

* One is a Wasm container for the Redis client app. The entire container image is only 0.7MB -- far smaller than any Linux container. 
* The second is a Linux container for the Redis server. 

```bash
docker compose up
```
