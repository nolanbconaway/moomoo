DOCKER_DIRS = ingest ml http dbt playlist

.PHONY:
docker-build-all:
	@echo "Building all docker images"
	for dir in $(DOCKER_DIRS); do \
		echo "Building docker image for $$dir"; \
        $(MAKE) -C $$dir docker-build; \
    done
	@echo "\nDone building all docker images. Versions:"
	@make print-docker-versions

.PHONY:
print-docker-versions:
	@for dir in $(DOCKER_DIRS); do \
		docker images | grep moomoo-$${dir}-v | awk '{print $$1}' | sort | tail -1; \
	done
