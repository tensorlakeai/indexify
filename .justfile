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

[doc('Reformat all components')]
fmt: fmt-rust fmt-indexify

[doc('Check formatting of Rust components')]
check-rust: nightly-toolchain
    rustup run nightly cargo fmt -- --check

[doc('Check formatting of Indexify')]
[working-directory: 'indexify']
check-indexify:
    poetry run black --check .
    poetry run isort . --check-only --profile black

[doc('Check formatting of all components')]
check: check-rust check-indexify

[doc('Build Rust components')]
build-rust:
    cargo build

[doc('Build Indexify')]
[working-directory: 'indexify']
build-indexify:
    make

[doc('Build all components')]
build: build-rust build-indexify

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
    ./run-tests.sh

[doc('Test all components')]
test: test-rust test-indexify
