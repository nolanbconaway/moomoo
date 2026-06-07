
VERSION := $(shell cat src/moomoo_pg/version)

.PHONY:
version:
	@ echo $(VERSION)

.PHONY:
lint:
	@ ruff check
	@ ruff format --check

.PHONY:
format:
	@ ruff check --select I --fix
	@ ruff format

.PHONY:
test:
	@ pytest

.PHONY:
lint-test: lint test

.PHONY:
docker-build:
	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-pg-v$(VERSION) .
