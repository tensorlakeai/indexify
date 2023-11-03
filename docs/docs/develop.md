# Developing Indexify

## Install Dependencies

Before you start, this doc may be outdated. Please follow the procedur in `run_tests.sh`, this is the source of truth.

### Rust Compiler

Install various rust related tools -

* Rust Compiler - <http://rustup.rs>
* Cargo tools - clippy, rustfmt is very helpful for formating and fixing warnings.

### Python Dependencies

Create a virtual env

```shell
python3.11 -m venv ve
source ve/bin/activate
```

Install the  extractors

```shell
pip install .
```

<!-- Because sometimes it will not work  pip install --upgrade --force-reinstall . -->

If you are working on the Python SDK then install the SDK as well

```shell
(cd sdk-py && pip install -e .)
```

The following workaround is needed until PyO3 can detect virtualenvs in OSX and some Ubuntu versions

```shell
 export PYTHONPATH=${PYTHONPATH}:${pwd}/ve/lib/python3.11/site-packages
```

### MAC OS

Install coreutils

```shell
brew install coreutils
```

## Running Tests

We currently depend on the Qdrant VectorDB and Postgres to test Indexify.

### Start Development Dependencies

```shell
make local-dev
```

### Run Tests

Run the unit and integration tests

```shell
cargo test -- --test-threads 1
```

## Running the service locally

### Build the Binary

Build the server in development mode

```shell
cargo build
```

### Create a development database

```shell
make local-dev
```

### Start the server

Once the binary is built start it with a default config -

```shell
./target/debug/indexify start-server -d -c local_config.yaml
```

## Visual Studio DevContainer

Visual Studio Code Devcontainers have been setup as well. Opening the codebase in VS Studio Code should prompt opening the project in a container. Once the container is up, test that the application can be compiled and run -

1. `make local-dev`
2. Install the Python Dependencies as described above.
3. Compile and Run the application as described above.

### Docker-compose

If you're within the dev container, you can call the docker-compose-v1 from within /usr/bin/
