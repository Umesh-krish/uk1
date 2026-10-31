# Everything below `demo` runs with no containers and no cloud account.
.DEFAULT_GOAL := help
export MATCHLAKE_DATA := $(CURDIR)/data

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install:  ## Install the Python dependencies
	pip install -r requirements.txt

seed:  ## Generate three seasons of source data
	python scripts/generate_seed_data.py

bronze:  ## Build the bronze Parquet layer (DuckDB, no Spark needed)
	python scripts/build_bronze_local.py

warehouse:  ## Run the dbt models
	@mkdir -p "$(MATCHLAKE_DATA)/warehouse"
	cd dbt && dbt run --profiles-dir . --target $${DBT_TARGET:-dev}

test:  ## Run the dbt test suite
	cd dbt && dbt test --profiles-dir . --target $${DBT_TARGET:-dev}

demo: seed bronze warehouse test query  ## Full offline run, start to finish
	@echo "\nPipeline complete."

query:  ## Print the league table from the gold layer
	@python scripts/show_league_table.py

docs:  ## Build and serve the dbt documentation site
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

up:  ## Start the full container stack (MinIO, Kafka, Spark, Airflow)
	docker compose up -d

down:  ## Stop the stack
	docker compose down -v

stream:  ## Replay a season's events into Kafka
	python ingestion/stream_producer.py --matches 5

clean:  ## Remove generated data
	rm -rf data/bronze data/warehouse data/_checkpoints dbt/target dbt/logs

.PHONY: help install seed bronze warehouse test demo query docs up down stream clean
