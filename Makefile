DOCKER_USERNAME ?= diptanu
APPLICATION_NAME ?= indexify

build-container:
	docker build --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune --force --filter label=stage=builder

entity:
	sea-orm-cli generate entity -o src/entity --with-serde both --date-time-crate time

