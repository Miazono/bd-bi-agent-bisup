SHELL := /bin/bash

.PHONY: up down restart load-raw load-bronze

init-storage:
	APP_ENV=local python -m scripts.init_storage

up:
	docker compose --env-file .env.docker up -d

down:
	docker compose down

restart: down up

load-bronze:
	APP_ENV=local python -m ingestion.load_bronze \
	--load-date 2026-03-08 \
	--batch-id hm_20260308_01

load-raw:
	APP_ENV=local python -m ingestion.load_raw \
	--load-date $(shell date +%Y-%m-%d) \
	--source-dir data/raw

load-silver:
	APP_ENV=local python -m ingestion.load_silver \
	--batch-id hm_20260308_01 \
	--stats-prefix-len 1
