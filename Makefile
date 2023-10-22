
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
