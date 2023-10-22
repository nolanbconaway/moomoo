
.PHONY: 
py-lint:
	@ black tests src --check --verbose

.PHONY: 
py-format:
	@ black tests src --verbose

.PHONY: 
py-test:
	@ CONTACT_EMAIL=FAKE pytest tests --verbose

.PHONY: 
py-lint-test: py-lint py-test

.PHONY:
http:
	@ PORT=$${port:-8080} HOST=$${host:-0.0.0.0} \
		&& python -m moomoo.http.app --port=$$PORT --host=$$HOST

.PHONY:
docker-http-serve:
# set PORT to the desired port to publish. default is 5600
# 
# set RUNOPT to --rm to remove the container after it exits (default), or something like
#   --restart=always to keep it running.
# 
# there absolutely must be a better way to do this.
	@ PORT=$${PORT:-5600} && \
	  echo Serving locally on http://localhost:$$PORT && \
	  docker run --interactive --tty --detach\
	  	$${RUNOPT:---rm} \
		--publish=$$PORT:8080 \
		--add-host=host.docker.internal:host-gateway \
		--env MOOMOO_POSTGRES_URI="$(MOOMOO_DOCKER_POSTGRES_URI)" \
		--env MOOMOO_DBT_SCHEMA=${MOOMOO_DBT_SCHEMA} \
		--env MOOMOO_INGEST_SCHEMA=${MOOMOO_INGEST_SCHEMA} \
		moomoo-v$$(moomoo version) \
		make http

.PHONY:
get-docker-logs:
	@ docker logs $$(docker ps | grep moomoo-v$(moomoo version) |  cut -d' ' -f1)

.PHONY:
docker-build:
# run tests if TEST=1
	@ if [ ! -z "$(TEST)" ]; \
		then make py-lint-test; \
		else echo "Skipping tests."; \
	  fi

	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-v$$(moomoo version) .
