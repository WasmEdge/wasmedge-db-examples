# wasmedge-mysql-driver

`wasmedge-mysql-driver` is a mysql connector written in Rust. It is modified from [rust-mysql-simple
](https://github.com/blackbeam/rust-mysql-simple) and can be compiled to WASM, then executed by [WasmEdge](https://github.com/WasmEdge/WasmEdge).

## Usage

You can compile and run the examples using the following commands:

```bash
cargo build --target wasm32-wasip1
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasip1/debug/query.wasm
wasmedge --env "DATABASE_URL=mysql://user:passwd@127.0.0.1:3306/mysql" target/wasm32-wasip1/debug/insert.wasm
```