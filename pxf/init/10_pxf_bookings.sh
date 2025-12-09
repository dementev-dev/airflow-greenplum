#!/usr/bin/env bash

# Простой init-скрипт для настройки PXF:
# - кладёт JDBC-драйвер PostgreSQL в каталог PXF;
# - копирует jdbc-site.xml для сервера bookings-db;
# - выполняет pxf cluster sync.
# Скрипт выполняется только при первой инициализации Greenplum (пустой /data).

set -euo pipefail

GREENPLUM_USER=${GREENPLUM_USER:-gpadmin}
PXF_BASE_DEFAULT="${GREENPLUM_DATA_DIRECTORY:-/data}/pxf"

# Пробуем подтянуть PXF_BASE из .bashrc пользователя Greenplum
if [ -f "/home/${GREENPLUM_USER}/.bashrc" ]; then
    # shellcheck disable=SC1090
    source "/home/${GREENPLUM_USER}/.bashrc"
fi

PXF_BASE=${PXF_BASE:-$PXF_BASE_DEFAULT}

mkdir -p "${PXF_BASE}/lib" "${PXF_BASE}/servers/bookings-db"

# Копируем JAR-драйвер, если он ещё не установлен
if [ -f /pxf-local/postgresql-42.7.3.jar ] && [ ! -f "${PXF_BASE}/lib/postgresql-jdbc.jar" ]; then
    cp /pxf-local/postgresql-42.7.3.jar "${PXF_BASE}/lib/postgresql-jdbc.jar"
fi

# Копируем jdbc-site.xml для сервера bookings-db, если его нет
if [ -f /pxf-local/servers/bookings-db/jdbc-site.xml ] && [ ! -f "${PXF_BASE}/servers/bookings-db/jdbc-site.xml" ]; then
    cp /pxf-local/servers/bookings-db/jdbc-site.xml "${PXF_BASE}/servers/bookings-db/jdbc-site.xml"
fi

# Синхронизируем конфигурацию PXF (на всякий случай)
if command -v pxf >/dev/null 2>&1; then
    pxf cluster sync || true
fi

