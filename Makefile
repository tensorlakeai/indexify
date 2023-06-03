DOCKER_USERNAME ?= diptanu
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

.PHONY: do_script
install-py:
	$(MAKE) -C ${src_py_dir} install_deps.sh

shell:
	docker run --net host -v ${current_dir}:/indexify-build/indexify -it diptanu/indexify-build /bin/bash

