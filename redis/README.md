# Examples for WasmEdge Redis Client

## Usage

```bash
cargo build --target wasm32-wasi --release
wasmedge --env "REDIS_URL=redis://localhost/" target/wasm32-wasi/release/wasmedge-redis-client-examples.wasm
```
