.PHONY: 
py-lint:
	black tests src --check --verbose

.PHONY: 
py-format:
	black tests src --verbose

.PHONY: 
py-test:
	pytest tests --verbose


.PHONY:
dbt-deps:
	cd dbt && dbt deps

.PHONY:
dbt-run:
	# NOTE: set select=... in the make command.
	if [ ! -z "$(select)" ]; then dbt run --project-dir dbt/ --select "$(select)"; else exit 1; fi

.PHONY:
dbt-test:
	# NOTE: set select=... in the make command.
	if [ ! -z "$(select)" ]; then dbt test --project-dir dbt/ --select "$(select)"; else exit 1; fi

.PHONY:
sql-lint:
	cd dbt && sqlfluff lint models \
		--config ../.sqlfluff \
		--disable-progress-bar



