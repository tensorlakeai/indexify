all: build

# .proto file and generated Python files have to be in the same directory.
# See known issue https://github.com/grpc/grpc/issues/29459.
FUNCTION_EXECUTOR_PROTO_DIR=indexify/function_executor/proto

build:
	@rm -rf dist
	@poetry install
	@poetry run python -m grpc_tools.protoc \
		--proto_path=. \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		${FUNCTION_EXECUTOR_PROTO_DIR}/function_executor.proto
	@#The generated proto files don't pass linter checks and need to get refomatted.
	@poetry run black ${FUNCTION_EXECUTOR_PROTO_DIR}
	@poetry run isort ${FUNCTION_EXECUTOR_PROTO_DIR} --profile black
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

version:
	@git add pyproject.toml
	@git commit -m "$$(poetry version -s)"
	@git tag --sign "v$$(poetry version -s)" -m "$(poetry version -s)"
	@git push --follow-tags

.PHONY: build format lint test version
