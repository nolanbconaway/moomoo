VERSION := $(shell yq .version dbt_project.yml)

.PHONY:
lint:
	@ sqlfluff lint models --config .sqlfluff --disable-progress-bar

.PHONY:
docker-build:
# run tests if TEST=1
	@ docker build -t moomoo-dbt-v$(VERSION) .

.PHONY:
docker-run:
	@ if [ -z "$(cmd)" ]; \
		then echo "ERROR: cmd=... (debug, deps, etc) is required."; exit 1; \
		else docker run --interactive --tty \
				--add-host=host.docker.internal:host-gateway \
				--env DBT_HOST="$(DBT_HOST)" \
				--env DBT_PORT="$(DBT_PORT)" \
				--env DBT_USER="$(DBT_USER)" \
				--env DBT_PASSWORD="$(DBT_PASSWORD)" \
				--env DBT_DBNAME="$(DBT_DBNAME)" \
				--env DBT_SCHEMA="$(DBT_SCHEMA)" \
				moomoo-dbt-v$(VERSION) \
				$(cmd); \
	  fi

