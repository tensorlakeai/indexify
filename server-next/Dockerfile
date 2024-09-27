FROM lukemathwalker/cargo-chef:latest-rust-1.79.0 AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y \
    software-properties-common \
    build-essential make cmake ca-certificates \
    clang gcc g++ \
    protobuf-compiler

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin indexify-server

# We do not need the Rust toolchain to run the binary!
FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/indexify-server /usr/local/bin

ENTRYPOINT ["/usr/local/bin/indexify-server"]
