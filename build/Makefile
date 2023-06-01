DOCKER_USERNAME ?= diptanu
APPLICATION_NAME ?= indexify-build

build-container:
	docker build --tag ${DOCKER_USERNAME}/${APPLICATION_NAME} .
	docker image prune --force



