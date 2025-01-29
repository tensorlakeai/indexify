all: build

build:
	@rm -rf dist
	@poetry install
	@poetry build

fmt:
	@poetry run black .
	@poetry run isort . --profile black

check:
	@poetry run black --check .
	@poetry run isort . --check-only --profile black

lint:
	@poetry run pylint ./indexify
	@poetry run black --check .

test:
	cd tests && ./run_tests.sh

.PHONY: all build build_proto fmt lint test
