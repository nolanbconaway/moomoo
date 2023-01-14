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
dbt-run:
	dbt run --project-dir dbt/

.PHONY:
dbt-test:
	dbt test --project-dir dbt/

.PHONY:
sql-lint:
	cd dbt && sqlfluff lint models \
		--config ../.sqlfluff \
		--disable-progress-bar


