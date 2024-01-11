DOCKER_USERNAME ?= tensorlake
APPLICATION_NAME ?= indexify
current_dir = $(shell pwd)
src_py_dir = $(shell pwd)/src_py

# Installation directory
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

# Target executable
TARGET = ./target/release/$(APPLICATION_NAME)

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

fmt:
	rustup run nightly cargo fmt

local-dev:
	docker stop indexify-local-postgres || true
	docker run --rm -p 5432:5432 --name=indexify-local-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=indexify -d ankane/pgvector
	timeout 90s bash -c "until docker exec indexify-local-postgres pg_isready ; do sleep 5 ; done"
	docker exec indexify-local-postgres psql -U postgres -c 'create database indexify_test;'
	DATABASE_URL=postgres://postgres:postgres@localhost:5432/indexify
	docker stop indexify-local-qdrant || true
	docker run --rm -p 6334:6334 -p 6333:6333 --name=indexify-local-qdrant -d -e QDRANT__SERVICE__GRPC_PORT="6334"  qdrant/qdrant:v1.4.1
	docker stop indexify-local-opensearch || true
	docker run --rm -p 9200:9200 -p 9600:9600 --name=indexify-local-opensearch -d -e "discovery.type=single-node" opensearchproject/opensearch:latest
	docker stop indexify-local-redis || true
	docker run --rm -p 6379:6379 --name=indexify-local-redis -d redis:latest

# Used for local development only - referenced by the "tls" field in local_server_config.yaml
local-dev-tls-insecure:
	@mkdir -p .dev-tls && \
	openssl req -x509 -newkey rsa:4096 -keyout .dev-tls/ca.key -out .dev-tls/ca.crt -days 365 -nodes -subj "/C=US/ST=TestState/L=TestLocale/O=IndexifyOSS/CN=localtestca" && \
	openssl req -new -newkey rsa:4096 -keyout .dev-tls/server.key -out .dev-tls/server.csr -nodes -subj "/C=US/ST=TestState/L=TestLocale/O=IndexifyOSS/CN=localhost" && \
	openssl x509 -req -in .dev-tls/server.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/server.crt -days 365 && \
	openssl req -new -nodes -out .dev-tls/client.csr -newkey rsa:2048 -keyout .dev-tls/client.key -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/client.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/client.crt -days 365 -extfile ./client_cert_config -extensions v3_ca

.PHONY: local-dev-tls-insecure

test:
	./run_tests.sh

.PHONY: do_script
install-py:
	$(MAKE) -C ${src_py_dir} install_deps.sh

shell:
	docker run --net host -v ${current_dir}:/indexify-build/indexify -it ${DOCKER_USERNAME}/indexify-build /bin/bash

serve-docs:
	(cd docs && mkdocs serve)

install: build-release
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET) $(DESTDIR)$(BINDIR)/$(APPLICATION_NAME)