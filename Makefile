
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
dbt-deps:
	@ cd dbt && dbt deps

.PHONY:
dbt-build:
	@ if [ ! -z "$(select)" ]; \
		then dbt build --project-dir dbt/ --select "$(select)"; \
		else echo "ERROR: select=... is required."; exit 1; \
	  fi

.PHONY:
dbt-run:
	@ if [ ! -z "$(select)" ]; \
		then dbt run --project-dir dbt/ --select "$(select)"; \
		else echo "ERROR: select=... is required."; exit 1; \
	  fi

.PHONY:
dbt-test:
	@ if [ ! -z "$(select)" ]; \
		then dbt test --project-dir dbt/ --select "$(select)"; \
		else echo "ERROR: select=... is required."; exit 1; \
	  fi

.PHONY:
sql-lint:
	@ cd dbt && sqlfluff lint models \
		--config ../.sqlfluff \
		--disable-progress-bar

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
		--env POSTGRES_DSN="$(DOCKER_POSTGRES_DSN)" \
		--env MOOMOO_DBT_SCHEMA=${DOCKER_DBT_PG_SCHEMA} \
		moomoo-v$$(moomoo version) \
		make http

.PHONY:
get-docker-logs:
	@ docker logs $$(docker ps | grep moomoo-v$(moomoo version) |  cut -d' ' -f1)

.PHONY:
docker-build:
	@ echo "Running python linting and tests..."
	@ make py-lint
	@ make py-test

	@ echo 
	@ echo 
	@ echo "Running dbt build..."
	@ dbt build --project-dir dbt/

	@ echo 
	@ echo 
	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-v$$(moomoo version) .
