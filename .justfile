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

[doc('Run a dev Indexify executor')]
[working-directory: 'indexify']
run-executor:
    poetry run indexify-cli executor --grpc-server-addr localhost:8901 --verbose
