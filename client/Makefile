
VERSION := $(shell cat src/moomoo_client/version)

.PHONY:
version:
	@ echo $(VERSION)

.PHONY:
lint:
	@ black tests src --check --verbose
	@ ruff tests src

.PHONY:
format:
	@ black tests src --verbose

.PHONY:
test:
	@ pytest tests --verbose

.PHONY:
lint-test: lint test
