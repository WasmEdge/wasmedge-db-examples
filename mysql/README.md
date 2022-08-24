# wasmedge-mysql-driver

`wasmedge-mysql-driver` is a mysql connector written in Rust. It is modified from [rust-mysql-simple
](https://github.com/blackbeam/rust-mysql-simple) and can be compiled to WASM, then executed by [WasmEdge](https://github.com/WasmEdge/WasmEdge).

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build --target wasm32-wasi
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasi/debug/query.wasm
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasi/debug/insert.wasm
```