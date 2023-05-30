DOCKER_USERNAME ?= diptanu
APPLICATION_NAME ?= indexify

build:
	cargo build

build-release:
	cargo build --release

clean:
	cargo clean

build-container:
	docker build --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune --force --filter label=stage=builder

entity:
	DATABASE_URL="sqlite://indexify.db" sea-orm-cli generate entity -o src/entity --with-serde both --date-time-crate time

migrate-dev:
	rm -rf indexify.db
	sqlite3 indexify.db "VACUUM;"
	cargo install sea-orm-cli
	DATABASE_URL="sqlite://indexify.db" sea-orm-cli migrate up

test:
	run_tests.sh

