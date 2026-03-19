SHELL := /bin/bash

.PHONY: up down restart up-wrenai down-wrenai logs-wrenai load-raw load-bronze load-silver load-marts

init-storage:
	APP_ENV=local python -m scripts.init_storage

up:
	docker compose --env-file .env.docker up -d

up-wrenai:
	docker compose --env-file .env.docker --profile wrenai up -d wren-ui

down:
	docker compose down

down-wrenai:
	docker compose --env-file .env.docker --profile wrenai stop wren-ui wren-ai-service qdrant ibis-server wren-engine bootstrap

logs-wrenai:
	docker compose --env-file .env.docker --profile wrenai logs -f --tail=100 wren-ui wren-ai-service wren-engine ibis-server qdrant

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

load-marts:
	APP_ENV=local python -m ingestion.load_marts
