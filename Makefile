DOCKER_USERNAME ?= tensorlake
APPLICATION_NAME ?= indexify
current_dir = $(shell pwd)
src_py_dir = $(shell pwd)/src_py

# Installation directory
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

# Target executable
TARGET = ./target/release/$(APPLICATION_NAME)

.PHONY: build
build: ## Build rust application
	cargo build

build-release: ## Build rust release
	cargo build --release

build-release-ubuntu20.04:
	cargo install cross
	cross build --target x86_64-unknown-linux-gnu --release

build-release-aarch64:
	cargo install cross
	cross build --target aarch64-unknown-linux-gnu --release

clean: ## Clean rust build artifacts
	cargo clean

build-container: ## Build container
	docker build -f dockerfiles/Dockerfile.server --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune -f

build-container-dev: ## Build container for local development
	docker build -f dockerfiles/Dockerfile.builder --tag ${DOCKER_USERNAME}/builder .
	docker build -f dockerfiles/Dockerfile.local --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .

push-container: ## Push container to docker hub
	docker buildx build -f dockerfiles/Dockerfile.server --platform linux/amd64,linux/arm64 --push --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .

build-ui: ## Build Indexify UI
	docker build -f dockerfiles/Dockerfile.ui --tag ${DOCKER_USERNAME}/indexify-ui .

fmt: ## Run rustfmt
	rustup run nightly cargo fmt

halp: help

help: ## Show help message for each target
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

local-dev: ## Run local development environment
	rm -rf /tmp/indexify*
	docker stop indexify-local-postgres || true
	docker run --rm -p 5432:5432 --name=indexify-local-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=indexify -d ankane/pgvector
	timeout 90s bash -c "until docker exec indexify-local-postgres pg_isready ; do sleep 5 ; done"
	docker exec indexify-local-postgres psql -U postgres -c 'create database indexify_test;'
	DATABASE_URL=postgres://postgres:postgres@localhost:5432/indexify
	docker stop indexify-local-qdrant || true
	docker run --rm -p 6334:6334 -p 6333:6333 --name=indexify-local-qdrant -d -e QDRANT__SERVICE__GRPC_PORT="6334"  qdrant/qdrant:v1.4.1
	docker stop indexify-local-redis || true
	docker run --rm -p 6379:6379 --name=indexify-local-redis -d redis:latest

local-dev-tls-insecure: ## Generate local development TLS certificates (insecure)
	@mkdir -p .dev-tls && \
	openssl req -x509 -newkey rsa:4096 -keyout .dev-tls/ca.key -out .dev-tls/ca.crt -days 365 -nodes -subj "/C=US/ST=TestState/L=TestLocale/O=IndexifyOSS/CN=localhost" && \
	openssl req -new -newkey rsa:4096 -keyout .dev-tls/server.key -out .dev-tls/server.csr -nodes -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/server.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/server.crt -days 365 -extensions v3_ca -extfile ./client_cert_config && \
	openssl req -new -nodes -out .dev-tls/client.csr -newkey rsa:2048 -keyout .dev-tls/client.key -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/client.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/client.crt -days 365 -extfile ./client_cert_config -extensions v3_ca

.PHONY: local-dev-tls-insecure

test:
	./run_tests.sh

.PHONY: do_script
install-py: ## Install python dependencies
	$(MAKE) -C ${src_py_dir} install_deps.sh

shell: ## Run shell in container
	docker run --net host -v ${current_dir}:/indexify-build/indexify -it ${DOCKER_USERNAME}/indexify-build /bin/bash

serve-docs: ## Serve documentation
	(cd docs && mkdocs serve)

install: build-release ## Build the application and install it to the system
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET) $(DESTDIR)$(BINDIR)/$(APPLICATION_NAME)

package-ui:
	cd ui && npm install && npm run build

build-docs:
	cd docs && pip install -r requirements.txt
	mkdir -p docs/docs/example_code
	cp -r examples/* docs/docs/example_code
	cd docs && mkdocs build

serve-docs:
	cd docs && pip install -r requirements.txt
	mkdir -p docs/docs/example_code
	cp -r examples/* docs/docs/example_code
	cd docs && mkdocs serve
