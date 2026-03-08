#!/bin/sh
# Создаёт бакет lakehouse при инициализации

set -e

# алиас для MinIO
mc alias set minio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb --ignore-existing "minio/${LAKEHOUSE_BUCKET:-lakehouse}"
