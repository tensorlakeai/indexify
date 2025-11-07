# https://just.systems

[private]
default:
    @just --list

[private]
nightly-toolchain:
    rustup toolchain install nightly
    rustup component add --toolchain nightly rustfmt

[doc('Reformat Rust components')]
fmt-rust: nightly-toolchain
    rustup run nightly cargo fmt

[doc('Reformat Indexify')]
[working-directory: 'indexify']
fmt-indexify:
    poetry run black .
    poetry run isort . --profile black

[doc('Reformat Tensorlake')]
[working-directory: 'tensorlake']
fmt-tensorlake:
    poetry run black .
    poetry run isort . --profile black

[doc('Reformat all components')]
fmt: fmt-rust fmt-indexify fmt-tensorlake

[doc('Check formatting of Rust components')]
check-rust: nightly-toolchain
    rustup run nightly cargo fmt -- --check

[doc('Check formatting of Indexify')]
[working-directory: 'indexify']
check-indexify:
    poetry run black --check .
    poetry run isort . --check-only --profile black

[doc('Check formatting of Tensorlake')]
[working-directory: 'tensorlake']
check-tensorlake:
    poetry run black --check .
    poetry run isort . --check-only --profile black

[doc('Check formatting of all components')]
check: check-rust check-indexify check-tensorlake

[doc('Build Rust components')]
build-rust:
    cargo build

[doc('Build Indexify')]
[working-directory: 'indexify']
build-indexify:
    make

[doc('Build Tensorlake')]
[working-directory: 'tensorlake']
build-tensorlake:
    make

[doc('Build all components')]
build: build-rust build-indexify build-tensorlake

[doc('Clean Rust components')]
clean-rust:
    cargo clean

[doc('Clean all components')]
clean: clean-rust

[doc('Test Rust components')]
test-rust:
    cargo test --workspace -- --test-threads 1

[doc('Test Indexify')]
[working-directory: 'indexify/tests']
test-indexify:
    ./run_tests.sh

[doc('Test Tensorlake')]
[working-directory: 'tensorlake/tests']
test-tensorlake:
    ./run_tests.sh

[doc('Test all components')]
test: test-rust test-indexify test-tensorlake

[doc('Run a local Indexify server')]
run-server:
    cargo run -p indexify-server

[doc('Run a local Indexify server with Tokio Console enabled')]
run-server-with-console:
    RUSTFLAGS="--cfg tokio_unstable" cargo run -p indexify-server --features console-subscriber

[doc('Run a dev Indexify executor')]
[working-directory: 'indexify']
run-executor:
    poetry run indexify-cli executor --grpc-server-addr localhost:8901 --verbose

[doc('Run Clippy on all Rust packages, marking warnings as errors')]
lint-rust:
    cargo clippy --no-deps -- -D warnings

[doc('Try to automatically fix Clippy problems')]
lint-fix:
    cargo clippy --no-deps --fix --allow-dirty

[doc('Run Jaeger to collect traces locally')]
run-jaeger:
    docker run -d --name jaeger \
        -e COLLECTOR_OTLP_ENABLED=true \
        -e COLLECTOR_OTLP_GRPC_HOST_PORT=0.0.0.0:4317 \
        -p 6831:6831/udp \
        -p 6832:6832/udp \
        -p 5778:5778 \
        -p 16686:16686 \
        -p 4317:4317 \
        -p 4318:4318 \
        -p 14250:14250 \
        -p 14268:14268 \
        -p 14269:14269 \
        -p 9411:9411 \
        jaegertracing/all-in-one:latest \
        --set=receivers.otlp.protocols.grpc.endpoint="0.0.0.0:4317"
