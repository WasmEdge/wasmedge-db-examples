# wasmedge-mysql-async-driver

`wasmedge-mysql-async-driver` is a async mysql connector written in Rust. It is modified from [mysql_async](https://github.com/blackbeam/mysql_async) and can be compiled to WASM, then executed by [WasmEdge](https://github.com/WasmEdge/WasmEdge).

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build --target wasm32-wasi
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasi/debug/crud.wasm
```

