
VERSION := $(shell cat src/moomoo_navidrome/version)

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
	@ docker build -t moomoo-navidrome-v$(VERSION) .

.PHONY:
docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... is required."; exit 1; \
		else docker run --interactive --tty --rm \
			--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
			--env MOOMOO_DBT_SCHEMA=$(MOOMOO_DBT_SCHEMA) \
			--env NAVIDROME_URL=$(NAVIDROME_URL) \
			--env NAVIDROME_USERNAME=$(NAVIDROME_USERNAME) \
			--env NAVIDROME_PASSWORD=$(NAVIDROME_PASSWORD) \
			--env NAVIDROME_DB_PATH=/navidrome.db \
			--env LISTENBRAINZ_USERNAME=$(LISTENBRAINZ_USERNAME) \
			--env LISTENBRAINZ_USER_TOKEN=$(LISTENBRAINZ_USER_TOKEN) \
			--add-host=host.docker.internal:host-gateway \
			--volume $(NAVIDROME_DB_PATH):/navidrome.db:ro \
			moomoo-navidrome-v$(VERSION) \
			moomoo-navidrome $(cmd); \
		fi