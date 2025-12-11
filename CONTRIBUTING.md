This guide will help you contribute to the project. It includes guidelines to run the project locally, how to test your changes integrating with function executors, and running more advanced tests.

We use [Just](https://just.systems/man/en/) to run commands locally. Once installed from the previous link, you can see all the commands available by running `just` at the root of this repository.

Once you have Just installed, you can run commands by typing `just <command>` at the root of this repository.
The command `just install-tools` will install additional tools required for development.

## Available Just Commands

## General

- `just install-tools` - Install additional tools required for development

### Formatting

- `just fmt` - Reformat all components (Rust, Indexify, and Tensorlake)
- `just fmt-rust` - Reformat Rust components using nightly toolchain
- `just fmt-indexify` - Reformat Indexify Python code using Black and isort
- `just fmt-tensorlake` - Reformat Tensorlake Python code using Black and isort

### Linting & Code Quality

- `just check` - Check formatting of all components without modifying them
- `just check-rust` - Check Rust formatting
- `just check-indexify` - Check Indexify Python formatting
- `just check-tensorlake` - Check Tensorlake Python formatting
- `just lint-rust` - Run Clippy on all Rust packages, treating warnings as errors
- `just lint-fix` - Automatically fix Clippy warnings (allows dirty working directory)

### Building

- `just build` - Build all components (Rust, Indexify, and Tensorlake)
- `just build-rust` - Build Rust components
- `just build-indexify` - Build Indexify
- `just build-tensorlake` - Build Tensorlake

### Testing

- `just test` - Run all tests (Rust, Indexify, and Tensorlake)
- `just test-rust` - Run Rust tests with single test thread
- `just test-indexify` - Run Indexify tests
- `just test-tensorlake` - Run Tensorlake tests

### Running Services

- `just run-server` - Run a local Indexify server on port 8900
- `just run-server-with-console` - Run server with Tokio Console enabled for debugging
- `just run-server-with-flamegraph` - Run server capturing flamegraphs
- `just run-executor` - Run a dev Indexify executor
- `just run-jaeger` - Run Jaeger locally for tracing collection

### Cleaning

- `just clean` - Clean Rust components
- `just clean-rust` - Clean Rust build artifacts

## Running Tensorlake benckmarks

1. Run the server with `just run-server`. This will compile the server locally and start it on port 8900.
2. In a separated terminal:
  - Create or enter a Python virtual environment. This can be done using `uv venv` and `source venv/bin/activate`.
  - Install `poetry` if you haven't already. We use it to manage dependencies.
  - Install the executor dependencies if you've never ran the executor before with `just build-indexify`.
  - Start the local executor with `just run-executor`. 
3. In a third terminal:
  - Export the `TENSORLAKE_API_URL=http://localhost:8900` environment variable.
  - Run the benchmarks with `just run-tl-benchmarks`.

If you want to change the configuration of the benchmarks, you can run the commands manually. For example, to run the map_reduce benchmark, run this:

```
cd indexify && \
  poetry run python3 benchmarks/map_reduce/main.py --maps-count 500 --num-requests 1 --failure-threshold-seconds 900
```

You can customize the number of requests, map jobs, and timeout threshold with the flags above.
