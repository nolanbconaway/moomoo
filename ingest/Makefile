
VERSION := $(shell cat src/moomoo_ingest/version)

.PHONY:
version:
	@ echo $(VERSION)

.PHONY:
lint:
	@ black tests src --check --verbose
	@ ruff .

.PHONY:
format:
	@ black tests src --verbose

.PHONY:
test:
	@ CONTACT_EMAIL=FAKE pytest tests --verbose

.PHONY:
lint-test: lint test

.PHONY:
docker-build:
	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-ingest-v$(VERSION) .


.PHONY:
docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... (listens --from=...) is required."; exit 1; \
		else docker run --interactive --tty --rm \
			--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
			--env MOOMOO_DBT_SCHEMA=$(MOOMOO_DBT_SCHEMA) \
			--env MOOMOO_CONTACT_EMAIL=$(MOOMOO_CONTACT_EMAIL) \
			--add-host=host.docker.internal:host-gateway \
			--mount=type=bind,source=$(MOOMOO_MEDIA_LIBRARY),target=/mnt/music,readonly \
			moomoo-ingest-v$(VERSION) \
			moomoo-ingest $(cmd); \
		fi