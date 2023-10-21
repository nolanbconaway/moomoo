VERSION := $(shell yq .version dbt_project.yml)

.PHONY:
lint:
	@ sqlfluff lint models --config .sqlfluff --disable-progress-bar

.PHONY:
docker-build:
# run tests if TEST=1
	@ if [ ! -z "$(TEST)" ]; \
		then dbt build --project-dir dbt/; \
		else echo "Skipping dbt build."; \
	  fi

	@ echo "\n\nBuilding docker image..."
	@ docker build -t moomoo-dbt-v$(VERSION) .

.PHONY:
docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... (debug, deps, etc) is required."; exit 1; \
		else docker run --interactive --tty \
				--add-host=host.docker.internal:host-gateway \
				--env DBT_SCHEMA="$(MOOMOO_DBT_SCHEMA)" \
				--env DBT_HOST="$(MOOMOO_DBT_HOST)" \
				--env DBT_PORT="$(MOOMOO_DBT_PORT)" \
				--env DBT_USER="$(MOOMOO_DBT_USER)" \
				--env DBT_PASSWORD="$(MOOMOO_DBT_PASSWORD)" \
				--env DBT_DBNAME="$(MOOMOO_DBT_DBNAME)" \
				moomoo-dbt-v$(VERSION) \
				$(cmd); \
	  fi

