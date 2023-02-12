# Database client examples for WasmEdge

Based on WasmEdge's [non-blocking network socket API](https://wasmedge.org/book/en/dev/rust/networking-nonblocking.html) and a Tokio-like [async runtime](https://github.com/WasmEdge/tokio_wasi), 
we are able to port popular database drivers and clients for the WasmEdge Runtime. This repo is a collection of examples accessing databases from inside WasmEdge applications.

* [MySQL / MariaDB / TiDB](mysql_async/) example
* [Postgres](postgres/) example
* [Redis](redis/) example

Alternatively, you can use the [Dapr SDK for Wasm](https://github.com/second-state/dapr-sdk-wasi) to access database or KV store services attached to your Dapr sidecars. [See examples here](https://github.com/second-state/dapr-wasm).

You can also check out 

* [A complete microservice demo](https://github.com/second-state/microservice-rust-mysql) with a HTTP server and a MySQL backend.
* [An event-driven microservice demo](https://github.com/docker/awesome-compose/tree/master/wasmedge-kafka-mysql) with a Kafka queue and a MySQL backend.
