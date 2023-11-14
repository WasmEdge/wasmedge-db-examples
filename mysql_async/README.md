# MySQL async example

In this example, we showcase how to make async (non blocking) connections to MySQL-compatible databases (e.g., MySQL, MariaDB, TiDB, Aurora etc.) from WasmEdge Rust apps. It utilizes the [mysql_async_wasi](https://github.com/WasmEdge/mysql_async_wasi) crate, which is derived from [mysql_async](https://github.com/blackbeam/mysql_async).

## Install

This example demonstrates making TLS connections to a remote MySQL server. It requires the WasmEdge TLS plugin. The command to install it is as follows.

```bash
curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- --plugins wasmedge_rustls
```

> Of course, you can connect to MySQL servers without TLS. Just remove `features = [ "default-rustls" ]` from `Cargo.toml` and `SslOpts` from the code. In that case, you will not need to install the `wasmedge_rustls` plugin.

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build --target wasm32-wasi
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasi/debug/crud.wasm
```
