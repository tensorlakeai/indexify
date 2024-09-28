#!/bin/bash

set -ex

(cd server && cargo test -- --test-threads 1)

echo "Ok, that is enough"

pip install poetry

(cd python-sdk && poetry install)
