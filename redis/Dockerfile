FROM --platform=$BUILDPLATFORM rust:1.88 AS buildbase
RUN rustup target add wasm32-wasip1
WORKDIR /src

FROM --platform=$BUILDPLATFORM buildbase AS buildclient
COPY . /src
RUN --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/cache \
    --mount=type=cache,target=/usr/local/cargo/registry/index \
    RUSTFLAGS="--cfg wasmedge --cfg tokio_unstable" cargo build --target wasm32-wasip1 --release

FROM scratch AS client
ENTRYPOINT [ "wasmedge-redis-client-examples.wasm" ]
COPY --link --from=buildclient /src/target/wasm32-wasip1/release/wasmedge-redis-client-examples.wasm  wasmedge-redis-client-examples.wasm
