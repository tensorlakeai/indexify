# Indexify Python Client

## Installation

This is the Python client for interacting with the Indexify service.

To install it, simply run:

```shell
pip install indexify
```

## Usage

See the [getting started](https://getindexify.com/getting_started/) guide for examples of how to use the client.
Look at the [sdk-py/examples](examples) directory for more examples.

## Development

Install and run the `poetry` package manager:

```shell
pip install poetry
poetry install
```

More information at [https://python-poetry.org/docs/](https://python-poetry.org/docs/).


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

IndexifyClient uses httpx under the hood, so there are many environment variables that can be used to configure the client. More information on supported environment variables can be found [here](https://www.python-httpx.org/environment_variables/).
