
VERSION := $(shell cat src/moomoo_playlist/version)

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
	@ pytest tests --verbose

.PHONY: 
lint-test: lint test

.PHONY:
docker-build:
	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-playlist-v$(VERSION) .

.PHONY:
docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... (loved-tracks ...) is required."; exit 1; \
		else docker run --interactive --tty --rm \
			--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
			--env MOOMOO_DBT_SCHEMA=$(MOOMOO_DBT_SCHEMA) \
			--env LISTENBRAINZ_USERNAME=$(LISTENBRAINZ_USERNAME) \
			--add-host=host.docker.internal:host-gateway \
			moomoo-playlist-v$(VERSION) \
			moomoo-playlist $(cmd); \
		fi