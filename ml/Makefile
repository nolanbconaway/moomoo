
.PHONY: 
lint:
	@ black tests src --check --verbose
	@ ruff tests src

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
	@ docker build -t moomoo-ml-v$$(moomoo-ml version) .

.PHONY:
docker-run:
	@ docker run \
		--interactive --tty --rm \
		--env MOOMOO_POSTGRES_URI=$(MOOMOO_DOCKER_POSTGRES_URI) \
		--env MOOMOO_ML_DEVICE=$(MOOMOO_ML_DEVICE) \
		--add-host=host.docker.internal:host-gateway \
		--mount=type=bind,source=$(MOOMOO_MEDIA_LIBRARY),target=/mnt/music,readonly \
		--gpus all \
		moomoo-ml-v$$(moomoo-ml version) \
		moomoo-ml score /mnt/music