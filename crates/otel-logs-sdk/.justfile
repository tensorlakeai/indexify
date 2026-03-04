# https://just.systems

[private]
default:
    @just --list

[private]
nightly-toolchain:
    rustup toolchain install nightly
    rustup component add --toolchain nightly rustfmt


[doc('Install the necessary tools to build and test Indexify')]
install-tools: && nightly-toolchain
    curl -LsSf https://astral.sh/uv/install.sh | sh
    uv venv --allow-existing
    uv tool install poetry
    cargo binstall -V > /dev/null || cargo install cargo-binstall
    cargo binstall cargo-nextest --locked --secure --no-confirm
    cargo binstall cargo-flamegraph --locked --secure --no-confirm

[doc('Reformat Rust components')]
fmt: nightly-toolchain
    rustup run nightly cargo fmt

[doc('Check formatting of Rust components')]
check: nightly-toolchain
    rustup run nightly cargo fmt -- --check

[doc('Build Rust components')]
build:
    cargo build --all-features

[doc('Clean Rust components')]
clean:
    cargo clean

[doc('Test Rust components')]
test:
    cargo nextest run --all-features

[doc('Run Clippy on all Rust packages, marking warnings as errors')]
lint:
    cargo clippy --no-deps --all-features -- -D warnings

[doc('Try to automatically fix Clippy problems')]
lint-fix:
    cargo clippy --no-deps --fix --allow-dirty --all-features
