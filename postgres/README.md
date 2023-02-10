# Postgres example

In this example, we showcase how to make async (non blocking) connections to Postgres databases from WasmEdge Rust apps. It utilizes the [tokio-postgres_wasi](https://github.com/WasmEdge/rust-postgres/tree/master/tokio-postgres) crate, which is derived from [tokio-postgres](https://github.com/sfackler/rust-postgres/tree/master/tokio-postgres).

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build --target wasm32-wasi --release
wasmedge --env "DATABASE_URL=postgres://postgres@localhost/postgres" target/wasm32-wasi/release/crud.wasm
```
