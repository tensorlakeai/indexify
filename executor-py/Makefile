all: build

build:
	@rm -rf dist
	@poetry build

fmt:
	black .
	isort . --profile black

lint:
	@poetry run pylint ./indexify_executor
	@poetry run black --check .

test:
	@poetry run pytest --cov=./indexify_executor

watch:
	@poetry run ptw

version:
	@git add pyproject.toml
	@git commit -m "$$(poetry version -s)"
	@git tag --sign "v$$(poetry version -s)" -m "$(poetry version -s)"
	@git push --follow-tags

.PHONY: build format lint test watch version
