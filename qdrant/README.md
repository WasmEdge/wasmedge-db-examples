# Qdrant vector database example

WasmEdge is emerging as a lightweight, portable, secure and cloud-native runtime for large language models (LLMs). LLM inference applications, such as RAG chatbots and AI agents, can be developed on Mac or Windows, compiled to Wasm once, and then deployed across Nvidia / AMD / ARM-powered devices or servers, fully taking advantage of on-device GPUs, NPUs, and accelerators.

Hence, besides the LLM inference runtime, those LLM applications also need to manage embeddings in vector databases. The [qdrant-rest-client](https://crates.io/crates/qdrant_rest_client) crate allows you to access the Qdrant vector database from your portable Wasm apps!

## Quick start

Install WasmEdge and Rust tools.

```
curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- --plugins wasmedge_rustls
source $HOME/.wasmedge/env

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup target add wasm32-wasi
```

Start a Qdrant instance in Docker using the quick start guide.

```
mkdir qdrant_storage

docker run -p 6333:6333 -p 6334:6334 \
    -v $(pwd)/qdrant_storage:/qdrant/storage:z \
    qdrant/qdrant
```

Build and run the example app.

```
cargo build --target wasm32-wasi --release
wasmedge target/wasm32-wasi/release/qdrant_examples.wasm
```
