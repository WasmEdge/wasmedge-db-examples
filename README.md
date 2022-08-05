# Database drivers for WasmEdge

Based on WasmEdge's [non-blocking network socket API](https://wasmedge.org/book/en/dev/rust/networking-nonblocking.html) and a Tokio-like [async runtime](https://github.com/WasmEdge/wasmedge-async), 
we are able to port popular database drivers and compile them to run on the WasmEdge Runtime. This repo is a collection of these drivers.
