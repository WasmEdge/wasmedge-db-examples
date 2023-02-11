# Postgres example

In this example, we showcase how to make async (non blocking) connections to Postgres databases from WasmEdge Rust apps. It utilizes the [tokio-postgres_wasi](https://github.com/WasmEdge/rust-postgres/tree/master/tokio-postgres) crate, which is derived from [tokio-postgres](https://github.com/sfackler/rust-postgres/tree/master/tokio-postgres).

## Linux CLI usage

Prerequisites:

* Install the Rust compiler and Cargo tools
* Install the WasmEdge Runtime
* Install and start the PostgreSQL server
  * Create a username `wasmedge` with password 'rocks`
  * Create a database `testdb` for user `wasmedge`

The following commands build and run the example.

```bash
cargo build --target wasm32-wasi --release
wasmedge --env "DATABASE_URL=postgres://wasmedge:rocks@localhost/testdb" target/wasm32-wasi/release/crud.wasm
```

## Docker usage

The following command builds and starts two containers. All you need is Docker Desktop. There is no need to install Rust or Redis or even WasmEdge.

* One is a Wasm container for the PostgreSQL client app. The entire container image is only 0.9MB -- far smaller than any Linux container.
* The second is a Linux container for the PostgreSQL server.

```bash
docker compose up
```
