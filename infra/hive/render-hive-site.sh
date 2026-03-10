#!/bin/sh
set -e

HIVE_CONF_DIR="${HIVE_CONF_DIR:-/opt/hive/conf}"
HIVE_SITE_FILE="${HIVE_CONF_DIR}/hive-site.xml"

if [ -z "${METASTORE_DB_NAME}" ] || [ -z "${METASTORE_DB_USER}" ] || [ -z "${METASTORE_DB_PASSWORD}" ]; then
  echo "Missing metastore DB env vars" >&2
  exit 1
fi

if [ -z "${HIVE_METASTORE_URI}" ]; then
  echo "Missing HIVE_METASTORE_URI env var" >&2
  exit 1
fi

cat > "${HIVE_SITE_FILE}" <<EOF
<configuration>
  <!-- PostgreSQL для хранения метаданных -->
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://metastore-db:5432/${METASTORE_DB_NAME}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${METASTORE_DB_USER}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${METASTORE_DB_PASSWORD}</value>
  </property>

  <!-- Адрес метастора -->
  <property>
    <name>hive.metastore.uris</name>
    <value>${HIVE_METASTORE_URI}</value>
  </property>

  <!-- warehouse-директория -->
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://lakehouse/warehouse/</value>
  </property>

  <!-- S3A подключение к MinIO -->
  <property>
    <name>fs.s3a.endpoint</name>
    <value>${S3_ENDPOINT}</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>${AWS_ACCESS_KEY_ID}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>${AWS_SECRET_ACCESS_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>

  <!-- Отключить проверку существования пути при создании схемы -->
  <property>
    <name>hive.metastore.check.valid.location</name>
    <value>false</value>
  </property>
</configuration>
EOF