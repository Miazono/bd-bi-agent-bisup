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
  <property>
    <name>hive.metastore.uris</name>
    <value>${HIVE_METASTORE_URI}</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/opt/hive/data/warehouse</value>
  </property>
</configuration>
EOF
