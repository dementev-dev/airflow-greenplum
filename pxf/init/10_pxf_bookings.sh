#!/usr/bin/env bash

# Идемпотентная подготовка PXF на каждом запуске контейнера:
# - копирует JDBC-драйвер и конфиг сервера из образа в PXF_BASE;
# - не перезаписывает файлы, если не задан PXF_SEED_OVERWRITE=1;
# - синхронизацию pxf выполняет только при PXF_SYNC_ON_START=1.

set -euo pipefail

PXF_SEED_DIR="${PXF_SEED_DIR:-/opt/pxf-seed}"
PXF_CONF_SEED_DIR="${PXF_CONF_SEED_DIR:-/usr/local/pxf/conf}"
PXF_BASE_DEFAULT="${GREENPLUM_DATA_DIRECTORY:-/data}/pxf"
PXF_BASE="${PXF_BASE:-$PXF_BASE_DEFAULT}"
PXF_SEED_OVERWRITE="${PXF_SEED_OVERWRITE:-0}"
PXF_SYNC_ON_START="${PXF_SYNC_ON_START:-0}"
PXF_CLI="${PXF_CLI:-/usr/local/pxf/bin/pxf}"
GP_USER="${GREENPLUM_USER:-gpadmin}"

log_info() {
    echo "INFO - $*"
}

log_warn() {
    echo "WARN - $*"
}

copy_seed_file() {
    local src="$1"
    local dst="$2"
    local label="$3"

    if [ ! -f "${src}" ]; then
        log_warn "seed-файл не найден: ${src}"
        return 0
    fi

    if [ "${PXF_SEED_OVERWRITE}" = "1" ] || [ ! -f "${dst}" ]; then
        cp -f "${src}" "${dst}"
        log_info "${label}: установлено в ${dst}"
        return 0
    fi

    log_info "${label}: уже существует, пропускаем"
}

mkdir -p "${PXF_BASE}/lib" "${PXF_BASE}/servers/bookings-db" "${PXF_BASE}/conf"
mkdir -p "${PXF_BASE}/run" "${PXF_BASE}/logs"

copy_seed_file \
    "${PXF_SEED_DIR}/postgresql-42.7.3.jar" \
    "${PXF_BASE}/lib/postgresql-jdbc.jar" \
    "JDBC драйвер PostgreSQL"

copy_seed_file \
    "${PXF_SEED_DIR}/servers/bookings-db/jdbc-site.xml" \
    "${PXF_BASE}/servers/bookings-db/jdbc-site.xml" \
    "jdbc-site.xml для bookings-db"

copy_seed_file \
    "${PXF_CONF_SEED_DIR}/pxf-application.properties" \
    "${PXF_BASE}/conf/pxf-application.properties" \
    "pxf-application.properties"

copy_seed_file \
    "${PXF_CONF_SEED_DIR}/pxf-env.sh" \
    "${PXF_BASE}/conf/pxf-env.sh" \
    "pxf-env.sh"

# Устанавливаем уменьшенные JVM-опции, если они ещё не заданы явно
if [ -f "${PXF_BASE}/conf/pxf-env.sh" ]; then
    if ! grep -Eq '^[[:space:]]*export[[:space:]]+PXF_JVM_OPTS=' "${PXF_BASE}/conf/pxf-env.sh"; then
        echo 'export PXF_JVM_OPTS="-Xmx512m -Xms256m"' >> "${PXF_BASE}/conf/pxf-env.sh"
        log_info "PXF_JVM_OPTS: установлен уменьшенный профиль памяти"
    fi
fi

copy_seed_file \
    "${PXF_CONF_SEED_DIR}/pxf-log4j2.xml" \
    "${PXF_BASE}/conf/pxf-log4j2.xml" \
    "pxf-log4j2.xml"

copy_seed_file \
    "${PXF_CONF_SEED_DIR}/pxf-profiles.xml" \
    "${PXF_BASE}/conf/pxf-profiles.xml" \
    "pxf-profiles.xml"

if [ "${PXF_SYNC_ON_START}" = "1" ]; then
    if [ ! -x "${PXF_CLI}" ]; then
        log_warn "pxf cli не найден: ${PXF_CLI}, пропускаем pxf cluster sync"
        exit 0
    fi

    # PXF CLI не запускается под root, поэтому выполняем синхронизацию под gpadmin.
    # PXF_BASE передаём явно, т.к. `su -` сбрасывает окружение.
    if ! su - "${GP_USER}" -c "PXF_BASE='${PXF_BASE}' '${PXF_CLI}' cluster sync"; then
        log_warn "pxf cluster sync завершился с ошибкой"
    else
        log_info "pxf cluster sync выполнен"
    fi
fi
