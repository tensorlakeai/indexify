# Developing Indexify

## Install Dependencies

### Rust Compiler
Install various rust related tools - 
* Rust Compiler - http://rustup.rs
* Cargo tools - clippy, rustfmt is very helpful for formating and fixing warnings.

### Python Dependencies
Create a virtual env 
```
python3.11 -m venv ve
source ve/bin/activate
```
Install the python server dependencies 
```
./install_python_deps.sh
```
Install the server libs 
```
(cd extractors && pip install .)
```
If you are working on the Python SDK then install the SDK as well 
```
(cd sdk-py && pip install .)
```

### MAC OS
The following workaround is needed until PyO3 can detect virtualenvs in OSX
```
 export PYTHONPATH=${PYTHONPATH}:${PWD}/ve/lib/python3.11/site-packages
```

Install coreutils 
```
brew install coreutils
```

## Running Tests
We currently depend on the Qdrant VectorDB and Postgres to test Indexify. 

### Start Development Dependencies
```
make local-dev
```

### Run Tests
Run the unit and integration tests
```
cargo test -- --test-threads 1
```

## Running the service locally 

### Build the Binary
Build the server in development mode 
```
cargo build
```

### Create a development database
```
make local-dev
```

### Start the server
Once the binary is built start it with a default config -
```
./target/debug/indexify start-server -d -c local_config.yaml
```

## Visual Studio DevContainer
Visual Studio Code Devcontainers have been setup as well. Opening the codebase in VS Studio Code should prompt opening the project in a container. Once the container is up, test that the application can be compiled and run -

1. `make local-dev`
2. Install the Python Dependencies as described above.
3. Compile and Run the application as described above.
