
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

docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... (listens --from=...) is required."; exit 1; \
		else docker run --interactive --tty --rm \
			--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
			--add-host=host.docker.internal:host-gateway \
			moomoo-pg-v$(VERSION) \
			moomoo-pg $(cmd); \
		fi