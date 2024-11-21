#!/bin/bash

set -ex

find . -name 'test_*.py' | xargs -L1 poetry run python
