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
	# NOTE: set tag=tag_name in the make command.
	if [ ! -z "$(tag)" ]; then dbt run --project-dir dbt/ --select tag:"$(tag)"; fi

.PHONY:
dbt-test:
	# NOTE: set tag=tag_name in the make command.
	if [ ! -z "$(tag)" ]; then dbt test --project-dir dbt/ --select tag:"$(tag)"; fi

.PHONY:
sql-lint:
	cd dbt && sqlfluff lint models \
		--config ../.sqlfluff \
		--disable-progress-bar



