# MySQL async example

In this example, we showcase how to make async (non blocking) connections to MySQL-compatible databases (e.g., MySQL, MariaDB, TiDB, Aurora etc.) from WasmEdge Rust apps. It utilizes the [mysql_async_wasi](https://github.com/WasmEdge/mysql_async_wasi) crate, which is derived from [mysql_async](https://github.com/blackbeam/mysql_async).

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build --target wasm32-wasi
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasi/debug/crud.wasm
```
