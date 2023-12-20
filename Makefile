DOCKER_USERNAME ?= tensorlake
APPLICATION_NAME ?= indexify
current_dir = $(shell pwd)
src_py_dir = $(shell pwd)/src_py

build:
	cargo build

build-release:
	cargo build --release

clean:
	cargo clean

build-container:
	docker build -f dockerfiles/Dockerfile.compose --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune --force --filter label=stage=builder


build-base-extractor:
	docker build -f dockerfiles/Dockerfile.extractor_base --tag ${DOCKER_USERNAME}/${APPLICATION_NAME}-extractor-base .

build-base-extractor-push:
	docker buildx build -f dockerfiles/Dockerfile.extractor_base --platform=linux/amd64,linux/arm64 --push --tag ${DOCKER_USERNAME}/${APPLICATION_NAME}-extractor-base .

push-container:
	docker buildx build -f dockerfiles/Dockerfile.compose --platform linux/amd64,linux/arm64 --push --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .

entity:
	sea-orm-cli generate entity -o src/entity --with-serde both --date-time-crate time

local-dev:
	docker stop indexify-local-postgres || true
	docker run --rm -p 5432:5432 --name=indexify-local-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=indexify -d ankane/pgvector
	timeout 90s bash -c "until docker exec indexify-local-postgres pg_isready ; do sleep 5 ; done"
	docker exec indexify-local-postgres psql -U postgres -c 'create database indexify_test;'
	cargo install sea-orm-cli
	DATABASE_URL=postgres://postgres:postgres@localhost:5432/indexify
	sea-orm-cli migrate up
	docker stop indexify-local-qdrant || true
	docker run --rm -p 6334:6334 -p 6333:6333 --name=indexify-local-qdrant -d -e QDRANT__SERVICE__GRPC_PORT="6334"  qdrant/qdrant:v1.4.1
	docker stop indexify-local-opensearch || true
	docker run --rm -p 9200:9200 -p 9600:9600 --name=indexify-local-opensearch -d -e "discovery.type=single-node" opensearchproject/opensearch:latest

test:
	./run_tests.sh

.PHONY: do_script
install-py:
	$(MAKE) -C ${src_py_dir} install_deps.sh

shell:
	docker run --net host -v ${current_dir}:/indexify-build/indexify -it diptanu/indexify-build /bin/bash

serve-docs:
	(cd docs && mkdocs serve)

fmt:
	cargo +nightly fmt
