# Indexify Python Client

## Installation

This is the Python client for interacting with the Indexify service.

To install it, simply run:

```shell
pip install indexify
```

## Usage

See the [getting started](https://getindexify.com/getting_started/) guide for examples of how to use the client.

## Development

For first time setup, follow the steps [here](https://getindexify.com/develop/).

### Steps for restarting dev server after updating server code

```shell
./install_python_deps.sh
# use `-e`` if you're developing extractors
(cd extractors && pip install -e .)
# use `-e`` if you're developing sdk-py
(cd sdk-py && pip install -e .)

cargo build
make local-dev

# start the server
./target/debug/indexify start-server -d -c local_config.yaml
```

### Environment Variables

IndexifyClient uses httpx under the hood, so you can set the following environment variables to configure the client to use https:

- `SSL_CERT_FILE`: path to a file containing the SSL certificate
- `SSL_CERT_DIR`: path to a directory containing SSL certificates

`httpx` also offers proxy support. More information on supported environment variables can be found [here](https://www.python-httpx.org/environment_variables/).
