add:
	@if [ -z "$(pkg)" ]; then \
		echo "Usage: make add pkg=<package_name> dep=<dependency_name>"; \
		exit 1; \
	fi

	@if [ -z "$(dep)" ]; then \
		echo "Usage: make add pkg=<package_name> dep=<dependency_name>"; \
		exit 1; \
	fi
	uv add --package transparentepstein-$(pkg) $(dep)

api:
	uv run api

migrate:
	uv run migrate

worker:
	uv run celery -A transparentepstein.ingestion.tasks worker --loglevel=info