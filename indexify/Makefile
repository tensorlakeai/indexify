all: build

build: build_proto
	@rm -rf dist
	@poetry install
	@poetry build

# .proto file and generated Python files have to be in the same directory.
# See known issue https://github.com/grpc/grpc/issues/29459.
PROTO_DIR_PATH_INSIDE_PACKAGE=indexify/function_executor/proto
PROTO_DIR_PATH=src/${PROTO_DIR_INSIDE_PACKAGE}

build_proto:
	@poetry install
	@cd src && poetry run python -m grpc_tools.protoc \
		--proto_path=. \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		${PROTO_DIR_PATH_INSIDE_PACKAGE}/function_executor.proto
	@#The generated proto files don't pass linter checks and need to get reformatted.
	@poetry run black ${PROTO_DIR_PATH}
	@poetry run isort ${PROTO_DIR_PATH} --profile black

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

.PHONY: build build_proto format lint test version
