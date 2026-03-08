#!/bin/sh
# Создаёт бакет lakehouse при инициализации

set -e

# Настраиваем алиас для MinIO
mc alias set minio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# Создаём один бакет для всех слоёв (raw/bronze/silver/marts)
mc mb --ignore-existing "minio/${LAKEHOUSE_BUCKET:-lakehouse}"
