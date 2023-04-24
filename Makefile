DOCKER_USERNAME ?= diptanu
APPLICATION_NAME ?= indexify

build-container:
	docker build --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune --force --filter label=stage=builder
