all: build

# .proto file and generated Python files have to be in the same directory.
# See known issue https://github.com/grpc/grpc/issues/29459.
PROTO_DIR_PATH_INSIDE_PACKAGE=function_executor/proto
PROTO_DIR_PATH=src/${PROTO_DIR_INSIDE_PACKAGE}

build:
	@rm -rf dist
	@poetry install
	@cd src && poetry run python -m grpc_tools.protoc \
		--proto_path=. \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		${PROTO_DIR_PATH_INSIDE_PACKAGE}/function_executor.proto
	@#The generated proto files don't pass linter checks and need to get refomatted.
	@poetry run black ${PROTO_DIR_PATH}
	@poetry run isort ${PROTO_DIR_PATH} --profile black
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
