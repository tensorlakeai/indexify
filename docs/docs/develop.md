# Developing Indexify

## Install Dependencies

### Rust Compiler
Install various rust related tools - 
* Rust Compiler - http://rustup.rs
* Cargo tools - clippy, rustfmt is very helpful for formating and fixing warnings.

### Python Dependencies
Create a virtual env 
```
virtualenv ve
source ve/bin/activate
```
Install the python server dependencies 
```
./install_python_deps.sh
```
Install the server libs 
```
(cd src_py && pip install .)
```

### MAC OS
The following workaround is needed until PyO3 can detect virtualenvs in OSX
```
 export PYTHONPATH=${PYTHONPATH}:${PWD}/ve/lib/python3.11/site-packages
```

## Running Tests
We currently have a hard dependency on the Qdrant VectorDB to test Indexify. 

### Start Qdrant
```
docker run -d -p 6333:6333 -p 6334:6334 -e QDRANT__SERVICE__GRPC_PORT="6334" qdrant/qdrant
```

### Run Tests
Run the unit and integration tests
```
cargo test
```

## Running the service locally 

## Build the Binary
Build the server in development mode 
```
cargo build
```

### Create a development database
```
make migrate-dev
```

### Start the server
Once the binary is built start it with a default config -
```
./target/debug/indexify start -c ./local_config.yaml
```

## Visual Studio DevContainer
Visual Studio Code Devcontainers have been setup as well. Opening the codebase in VS Studio Code should prompt opening the project in a container.
