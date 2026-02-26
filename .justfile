# https://just.systems

[private]
default:
    @just --list

[private]
nightly-toolchain:
    rustup toolchain install nightly
    rustup component add --toolchain nightly rustfmt

# Install the necessary tools to build and test Indexify
install-tools: && nightly-toolchain
    curl -LsSf https://astral.sh/uv/install.sh | sh
    uv venv --allow-existing
    uv tool install poetry
    cargo binstall -V > /dev/null || cargo install cargo-binstall
    cargo binstall cargo-nextest --locked --secure --no-confirm
    cargo binstall cargo-flamegraph --locked --secure --no-confirm

# Reformat Rust components
fmt-rust: nightly-toolchain
    rustup run nightly cargo fmt

# Reformat Indexify
[working-directory('indexify')]
fmt-indexify:
    poetry run black .
    poetry run isort . --profile black

# Reformat Tensorlake
[working-directory('tensorlake')]
fmt-tensorlake:
    poetry run black .
    poetry run isort . --profile black

# Reformat all components
fmt: fmt-rust fmt-indexify fmt-tensorlake

# Check formatting of Rust components
check-rust: nightly-toolchain
    rustup run nightly cargo fmt -- --check

# Check formatting of Indexify
[working-directory('indexify')]
check-indexify:
    poetry run black --check .
    poetry run isort . --check-only --profile black

# Check formatting of Tensorlake
[working-directory('tensorlake')]
check-tensorlake:
    poetry run black --check .
    poetry run isort . --check-only --profile black

# Check formatting of all components
check: check-rust check-indexify check-tensorlake

# Build Rust components
build-rust:
    cargo build

# Build Indexify
[working-directory('indexify')]
build-indexify:
    make

# Build Tensorlake
[working-directory('tensorlake')]
build-tensorlake:
    make

# Build all components
build: build-rust build-indexify build-tensorlake

# Clean Rust components
clean-rust:
    cargo clean

# Clean all components
clean: clean-rust

# Test Rust components
test-rust:
    cargo nextest run

# Test Indexify
[working-directory('indexify/tests')]
test-indexify:
    ./run_tests.sh

# Test Tensorlake
[working-directory('tensorlake/tests')]
test-tensorlake:
    ./run_tests.sh

# Test all components
test: test-rust test-indexify test-tensorlake

# Run a local Indexify server
run-server:
    cargo run -p indexify-server

# Run a local Indexify server with Tokio Console enabled
run-server-with-console:
    RUSTFLAGS="--cfg tokio_unstable" cargo run -p indexify-server --features console-subscriber

# Run a local Indexify server capturing flamegraphs
run-server-with-flamegraph:
    cargo flamegraph -p indexify-server

# Run a dev Indexify executor
[working-directory('indexify')]
run-executor:
    poetry run indexify-cli executor --grpc-server-addr localhost:8901 --verbose

# Run Clippy on all Rust packages, marking warnings as errors
lint-rust:
    cargo clippy --no-deps -- -D warnings

# Try to automatically fix Clippy problems
lint-fix:
    cargo clippy --no-deps --fix --allow-dirty

# Run Jaeger to collect traces locally
run-jaeger:
    docker run -d --rm --name jaeger \
      -p 16686:16686 \
      -p 4317:4317 \
      -p 4318:4318 \
      -p 5778:5778 \
      -p 9411:9411 \
      cr.jaegertracing.io/jaegertracing/jaeger:2.13.0

# Run Tensorlake benchmarks
[working-directory('indexify')]
run-tl-benchmarks maps_count="10" num_requests="10":
    poetry run python3 benchmarks/map_reduce/main.py --maps-count {{maps_count}} --num-requests {{num_requests}} --failure-threshold-seconds 900

# Build a Docker image for the Indexify server
build-server-image TAG="latest":
    docker build -t indexify-server:{{ TAG }} -f dockerfiles/Dockerfile.server .
