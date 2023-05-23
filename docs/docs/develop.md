# Developing Indexify

## Install LibTorch
Libtorch is required. It has to be manually downloaded for now.

### M1 Macs
PyTorch doesn't list the Libtorch packages on their homepages so libtorch has to be installed through homebrew
```
brew install pytorch
```
Once libtorch is installed, set the following paths 
```
export LIBTORCH=/opt/homebrew/Cellar/pytorch/2.0.0/
export LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH
```

### Linux 
Download from Pytorch homepage and unzip locally
```
wget https://download.pytorch.org/libtorch/cpu/libtorch-cxx11-abi-shared-with-deps-2.0.1%2Bcpu.zip
unzip libtorch-cxx11-abi-shared-with-deps-2.0.1+cpu.zip
```

Set the paths 
```
export LIBTORCH=/path/to/libtorch/
export LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH
```


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
