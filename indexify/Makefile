all: build

build: build_proto
	@rm -rf dist
	@poetry install
	@poetry build

TASK_SCHEDULER_PY_CLIENT_PROTO_DIR_PATH=indexify/proto
TASK_SCHEDULER_SERVER_PROTO_DIR_PATH=../server/internal_api/proto

build_proto: ${TASK_SCHEDULER_SERVER_PROTO_DIR_PATH}/task_scheduler.proto
	@poetry install
	@# .proto file and generated Python files have to be in the same directory.
	@# See known issue https://github.com/grpc/grpc/issues/29459.
	@cp ${TASK_SCHEDULER_SERVER_PROTO_DIR_PATH}/task_scheduler.proto src/${TASK_SCHEDULER_PY_CLIENT_PROTO_DIR_PATH}/task_scheduler.proto
	@cd src && poetry run python -m grpc_tools.protoc \
		--proto_path=. \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		${TASK_SCHEDULER_PY_CLIENT_PROTO_DIR_PATH}/task_scheduler.proto
	@#The generated proto files don't pass linter checks and need to get reformatted.
	@poetry run black src/${TASK_SCHEDULER_PY_CLIENT_PROTO_DIR_PATH}
	@poetry run isort src/${TASK_SCHEDULER_PY_CLIENT_PROTO_DIR_PATH} --profile black

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
