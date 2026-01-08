#!/usr/bin/env bash
set -euo pipefail

ensure_script="/opt/pxf-scripts/ensure_pxf_bookings.sh"

ensure_pxf_extension() {
    local gp_user="${GREENPLUM_USER:-gpadmin}"
    local gp_db="${GREENPLUM_DATABASE_NAME:-gp_dwh}"
    local gp_password="${GREENPLUM_PASSWORD:-gpadmin}"
    local attempts=60

    if [ "${GREENPLUM_PXF_ENABLE:-false}" != "true" ]; then
        return 0
    fi

    for _ in $(seq 1 "${attempts}"); do
        if /usr/local/greenplum-db/bin/pg_isready \
            -h 127.0.0.1 -p 5432 -U "${gp_user}" -d "${gp_db}" >/dev/null 2>&1; then
            if PGPASSWORD="${gp_password}" /usr/local/greenplum-db/bin/psql \
                -h 127.0.0.1 -p 5432 -U "${gp_user}" -d "${gp_db}" \
                -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pxf;" >/dev/null; then
                echo "INFO - extension pxf готово"
            else
                echo "WARN - не удалось создать extension pxf"
            fi
            return 0
        fi
        sleep 2
    done

    echo "WARN - Greenplum не готов, пропускаем CREATE EXTENSION pxf"
}

if [ -f "${ensure_script}" ]; then
    "${ensure_script}"
else
    echo "WARN - не найден ensure-скрипт PXF: ${ensure_script}"
fi

ensure_pxf_extension &

exec /start_gpdb.sh
