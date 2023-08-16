# Indexify Python Client

## Development

For first time setup, follow the steps [here](https://getindexify.com/develop/)

### Steps for restarting dev server after updating server code

```shell
./install_python_deps.sh
# use `-e`` if you're developing extractors
(cd extractors && pip install -e .)
# use `-e`` if you're developing sdk-py
(cd sdk-py && pip install -e .)

make local-dev
cargo build

# start the server
./target/debug/indexify start-server -d -c local_config.yaml
```
