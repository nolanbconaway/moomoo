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



