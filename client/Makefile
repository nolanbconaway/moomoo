
VERSION := $(shell cat src/moomoo_client/version)

.PHONY:
version:
	@ echo $(VERSION)

.PHONY: 
lint:
	@ ruff check
	@ ruff format --check

.PHONY: 
format:
	@ ruff check --select I --fix .
	@ ruff format

.PHONY:
test:
	@ pytest tests --verbose

.PHONY: 
lint-test: lint test

.PHONY:
lint-test: lint test


.PHONY:
docker-build:
	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-client-v$(VERSION) .

.PHONY:
docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... is required."; exit 1; \
		else docker run --interactive --rm \
			--env MOOMOO_HOST=$(MOOMOO_DOCKER_HOST) \
			--env MOOMOO_MEDIA_LIBRARY=/music \
			--env LISTENBRAINZ_USERNAME=$(LISTENBRAINZ_USERNAME) \
			--add-host=host.docker.internal:host-gateway \
			--mount=type=bind,source=$(MOOMOO_MEDIA_LIBRARY),target=/music,readonly \
			moomoo-client-v$(VERSION) \
			moomoo-client $(cmd); \
		fi
