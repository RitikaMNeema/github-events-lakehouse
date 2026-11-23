# GH Archive Lakehouse
SHELL := /bin/bash
DC := docker compose
EXEC := $(DC) exec -T airflow-scheduler

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------- lifecycle ----------
up: ## Build and start the stack (Docker Desktop must be running)
	$(DC) up -d --build
	@echo "Airflow  http://localhost:8081  (admin/admin)"
	@echo "MinIO    http://localhost:9001  (see .env)"
	@echo "Trino    http://localhost:8080"

down: ## Stop the stack, keep data
	$(DC) down

nuke: ## Stop the stack and DELETE all lake data + metadata
	$(DC) down -v

ps: ## Service status
	$(DC) ps

logs: ## Tail scheduler logs
	$(DC) logs -f airflow-scheduler

shell: ## Shell inside the pipeline container
	$(DC) exec airflow-scheduler bash

# ---------- pipeline, runnable without Airflow ----------
profile: ## Measure real file sizes / row counts for the docs
	$(EXEC) python ingest/profile_slice.py --hours 3

ingest: ## Fetch the raw slice defined in .env into the raw zone
	$(EXEC) python ingest/fetch_gharchive.py --from-env

bronze: ## Raw JSON -> Iceberg bronze (idempotent partition overwrite)
	$(EXEC) python spark/jobs/bronze_raw_to_iceberg.py --from-env

silver: ## Bronze -> typed/normalised silver + quarantine
	$(EXEC) python spark/jobs/silver_events.py --from-env

compact: ## Rewrite small files, expire old snapshots
	$(EXEC) python spark/jobs/compaction.py --table bronze.gh_events

dbt-deps: ## Install dbt packages (dbt_utils). Required once before dbt-run.
	$(EXEC) bash -c "cd /opt/project/dbt && dbt deps --profiles-dir ."

dbt-run: ## Build the gold star schema via Trino
	$(EXEC) bash -c "cd /opt/project/dbt && dbt build --profiles-dir ."

dbt-snapshot: ## Advance the SCD2 repo dimension
	$(EXEC) bash -c "cd /opt/project/dbt && dbt snapshot --profiles-dir ."

throughput: ## Bytes-processed ledger + the claims it supports
	$(EXEC) python tests/report_throughput.py

expire-raw: ## DRY RUN: show which raw hours could be expired, and why not
	$(EXEC) python spark/jobs/expire_raw.py --from-env

expire-raw-apply: ## DELETE raw for hours that pass all four safety gates
	$(EXEC) python spark/jobs/expire_raw.py --from-env --apply

quality: ## Run the standalone quality gate
	$(EXEC) python tests/quality_gate.py --from-env

all: ingest bronze silver dbt-deps dbt-snapshot dbt-run quality throughput ## Full pipeline on the .env slice

# ---------- the demos that make the project ----------
idempotency: ## Prove reprocessing is safe: run bronze twice, compare counts
	$(EXEC) python tests/prove_idempotency.py --from-env

benchmark: ## Partition-layout + compaction benchmark -> docs/_measured/
	$(EXEC) python tests/benchmark_layout.py

incident: ## Inject bad data and show the quality gate catching it
	$(EXEC) python tests/inject_incident.py

backfill: ## Backfill a date range through Airflow. START=.. END=..
	$(DC) exec -T airflow-scheduler airflow dags backfill \
	  --start-date $(START) --end-date $(END) gharchive_ingest

.PHONY: help up down nuke ps logs shell profile ingest bronze silver compact \
        dbt-deps dbt-run dbt-snapshot quality throughput expire-raw expire-raw-apply \
        all idempotency benchmark incident backfill
