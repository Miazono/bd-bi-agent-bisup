SHELL := /bin/bash

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: loadenv up down restart load-raw

up:
	docker compose up -d

down:
	docker compose down

restart: down up

load-raw: loadenv
	python ingestion/load_raw.py --load-date $(shell date +%Y-%m-%d) --source-dir data/raw
load-bronze:
	python -m ingestion.load_bronze \
	--load-date 2026-03-08 \
	--batch-id hm_20260308_01
loadenv:
	@set -a && source .env && set +a
