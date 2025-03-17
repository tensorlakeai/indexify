all: build

build: build_proto
	@rm -rf dist
	@poetry install
	@poetry build

SERVER_API_PY_CLIENT_PROTO_DIR_PATH=indexify/proto
SERVER_API_PROTO_DIR_PATH=../server/proto

build_proto: ${SERVER_API_PROTO_DIR_PATH}/executor_api.proto
	@poetry install
	@# .proto file and generated Python files have to be in the same directory.
	@# See known issue https://github.com/grpc/grpc/issues/29459.
	@cp ${SERVER_API_PROTO_DIR_PATH}/executor_api.proto src/${SERVER_API_PY_CLIENT_PROTO_DIR_PATH}/executor_api.proto
	@cd src && poetry run python -m grpc_tools.protoc \
		--proto_path=. \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		${SERVER_API_PY_CLIENT_PROTO_DIR_PATH}/executor_api.proto
	@#The generated proto files don't pass linter checks and need to get reformatted.
	@poetry run black src/${SERVER_API_PY_CLIENT_PROTO_DIR_PATH}
	@poetry run isort src/${SERVER_API_PY_CLIENT_PROTO_DIR_PATH} --profile black

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
