DOCKER_USERNAME ?= tensorlake
APPLICATION_NAME ?= indexify-server
current_dir = $(shell pwd)

# Installation directory
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

# Target executable
TARGET = ../target/release/$(APPLICATION_NAME)

.PHONY: build
build: ## Build rust application
	cargo build

build-release: ## Build rust release
	cargo install cross
	# cargo install cross --git https://github.com/cross-rs/cross
	cross build --target x86_64-unknown-linux-gnu --release

build-release-aarch64:
	cargo install cross
	# cargo install cross --git https://github.com/cross-rs/cross
	cross build --target aarch64-unknown-linux-gnu --release

clean: ## Clean rust build artifacts
	cargo clean

build-container: ## Build container
	docker build -f dockerfiles/Dockerfile.server --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune -f

build-container-dev: ## Build container for local development
	docker build -f dockerfiles/Dockerfile.local --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .

push-container: ## Push container to docker hub
	docker buildx build -f dockerfiles/Dockerfile.server --platform linux/amd64,linux/arm64 --push --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .

build-ui: ## Build Indexify UI
	docker build -f dockerfiles/Dockerfile.ui --tag ${DOCKER_USERNAME}/indexify-ui .

nightly-toolchain:
	rustup toolchain install nightly
	rustup component add --toolchain nightly rustfmt

.PHONY: fmt
fmt: nightly-toolchain ## Run rustfmt
	rustup run nightly cargo fmt

.PHONY: check
check: nightly-toolchain ## Run rustfmt check
	rustup run nightly cargo fmt -- --check

halp: help

help: ## Show help message for each target
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

local-dev-tls-insecure: ## Generate local development TLS certificates (insecure)
	@mkdir -p .dev-tls && \
	openssl req -x509 -newkey rsa:4096 -keyout .dev-tls/ca.key -out .dev-tls/ca.crt -days 365 -nodes -subj "/C=US/ST=TestState/L=TestLocale/O=IndexifyOSS/CN=localhost" && \
	openssl req -new -newkey rsa:4096 -keyout .dev-tls/server.key -out .dev-tls/server.csr -nodes -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/server.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/server.crt -days 365 -extensions v3_ca -extfile ./client_cert_config && \
	openssl req -new -nodes -out .dev-tls/client.csr -newkey rsa:2048 -keyout .dev-tls/client.key -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/client.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/client.crt -days 365 -extfile ./client_cert_config -extensions v3_ca

.PHONY: local-dev-tls-insecure

install: build-release ## Build the application and install it to the system
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET) $(DESTDIR)$(BINDIR)/$(APPLICATION_NAME)

package-ui:
	cd ui && npm install && npm run build

test:
	cargo test --workspace -- --test-threads 1

generate-cloud-openapi:
	@echo "Generating OpenAPI spec for cloud deployment..."
	@cargo run -- --gen-public-openapi

