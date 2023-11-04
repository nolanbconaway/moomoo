
VERSION := $(shell cat src/moomoo_http/version)

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
	@ CONTACT_EMAIL=FAKE pytest tests --verbose

.PHONY: 
lint-test: lint test

.PHONY:
http:
	@ PORT=$${port:-8080} HOST=$${host:-0.0.0.0} \
		&& moomoo-http serve --port=$$PORT --host=$$HOST

.PHONY:
docker-http-serve:
# set PORT to the desired port to publish. default is 5600
# 
# set RUNOPT to --rm to remove the container after it exits (default), or something like
#   --restart=always to keep it running.
# 
# there absolutely must be a better way to do this.
	PORT=$${PORT:-5600} && \
	  echo Serving locally on http://localhost:$$PORT && \
	  docker run --interactive --tty --detach\
	  	$${RUNOPT:---rm} \
		--publish=$$PORT:8080 \
		--add-host=host.docker.internal:host-gateway \
		--env MOOMOO_POSTGRES_URI="$(MOOMOO_DOCKER_POSTGRES_URI)" \
		--env MOOMOO_DBT_SCHEMA=${MOOMOO_DBT_SCHEMA} \
		moomoo-http-v$(VERSION) \
		make http

.PHONY:
get-docker-logs:
	@ docker logs $$(docker ps | grep moomoo-http-v$(VERSION) |  cut -d' ' -f1)

.PHONY:
docker-build:
	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-http-v$(VERSION) .
