#!/usr/bin/env bash
set -euo pipefail

ensure_script="/opt/pxf-scripts/ensure_pxf_bookings.sh"

# PXF CLI использует libpq, без PGPASSWORD после stop/start возможна ошибка auth.
if [ -z "${PGPASSWORD:-}" ]; then
    export PGPASSWORD="${GREENPLUM_PASSWORD:-gpadmin}"
fi

ensure_pxf_extension() {
    local gp_user="${GREENPLUM_USER:-gpadmin}"
    local gp_db="${GREENPLUM_DATABASE_NAME:-gp_dwh}"
    local gp_password="${GREENPLUM_PASSWORD:-gpadmin}"
    local attempts=60

    if [ "${GREENPLUM_PXF_ENABLE:-false}" != "true" ]; then
        return 0
    fi

    extension_exists() {
        local result
        result=$(PGPASSWORD="${gp_password}" /usr/local/greenplum-db/bin/psql \
            -h 127.0.0.1 -p 5432 -U "${gp_user}" -d "${gp_db}" \
            -t -A -c "SELECT 1 FROM pg_extension WHERE extname='pxf';" 2>/dev/null || true)
        [ "${result}" = "1" ]
    }

    for attempt in $(seq 1 "${attempts}"); do
        if /usr/local/greenplum-db/bin/pg_isready \
            -h 127.0.0.1 -p 5432 -U "${gp_user}" -d "${gp_db}" >/dev/null 2>&1; then
            if extension_exists; then
                echo "INFO - extension pxf уже создано"
                return 0
            fi
            if PGPASSWORD="${gp_password}" /usr/local/greenplum-db/bin/psql \
                -h 127.0.0.1 -p 5432 -U "${gp_user}" -d "${gp_db}" \
                -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pxf;" >/dev/null 2>&1; then
                if extension_exists; then
                    echo "INFO - extension pxf готово"
                    return 0
                fi
            fi
            echo "WARN - попытка ${attempt}/${attempts}: не удалось создать extension pxf"
        fi
        sleep 2
    done

    echo "WARN - Greenplum не готов или extension pxf не создано"
}

if [ -f "${ensure_script}" ]; then
    "${ensure_script}"
else
    echo "WARN - не найден ensure-скрипт PXF: ${ensure_script}"
fi

ensure_pxf_extension &

exec /start_gpdb.sh
