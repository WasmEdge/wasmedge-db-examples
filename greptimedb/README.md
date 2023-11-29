# GreptimeDB Example

This demo describes how to connect to GreptimeDB using MySQL protocol, on WasmEdge. It utilizes the [mysql_async_wasi](https://github.com/WasmEdge/mysql_async_wasi) crate, which is derived from [mysql_async](https://github.com/blackbeam/mysql_async).

## Setup

You can install WasmEdge with:

```bash
curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s
```

And create your own GreptimeDB instance on
[GreptimeCloud](https://greptime.cloud),
or use [self-hosted](https://github.com/GrepTimeTeam/greptimedb) version.

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build
wasmedge --env "DATABASE_URL=mysql://user:passwd@host:4002/dbname" target/wasm32-wasi/debug/greptimedb.wasm
```
