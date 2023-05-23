# Developing Indexify


## Running Tests
We currently have a hard dependency on the Qdrant VectorDB to test Indexify. 

### Start Qdrant
```
docker run -d -p 6333:6333 -p 6334:6334     -e QDRANT__SERVICE__GRPC_PORT="6334"     qdrant/qdrant
```

### Run Tests
Run the unit and integration tests
```
cargo test
```

### Running the service locally 
Build the server in development mode 
```
cargo build
```

Once the binary is built start it with a default config -
```
./target/debug/indexify start -c ./sample_config.yaml
```
