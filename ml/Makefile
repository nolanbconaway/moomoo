
VERSION := $(shell cat src/moomoo_ml/version)

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
	@ docker build -t moomoo-ml-v$(VERSION) .

.PHONY:
docker-run-scorer:
	@ docker run \
		--interactive --tty --rm \
		--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
		--env MOOMOO_ML_DEVICE=$(MOOMOO_ML_DEVICE) \
		--add-host=host.docker.internal:host-gateway \
		--mount=type=bind,source=$(MOOMOO_MEDIA_LIBRARY),target=/mnt/music,readonly \
		--gpus all \
		moomoo-ml-v$(VERSION) \
		moomoo-ml scorer score /mnt/music

.PHONY:
docker-run-conditioner:
	@ docker run \
		--interactive --tty --rm \
		--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
		--add-host=host.docker.internal:host-gateway \
		moomoo-ml-v$(VERSION) \
		moomoo-ml conditioner score