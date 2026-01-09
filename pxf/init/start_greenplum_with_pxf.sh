#!/usr/bin/env bash
set -euo pipefail

# Этот скрипт — "обёртка" над стандартным стартом Greenplum (exec /start_gpdb.sh),
# которая добавляет устойчивый старт PXF для учебного стенда.
#
# Задачи скрипта (почему он нужен):
# 1) Подготовить PXF_BASE на persistent volume (через ensure-скрипт).
# 2) Обеспечить стабильную аутентификацию PXF → GPDB после `docker compose stop/start`
#    (не зависеть от «плавающего» IP контейнера в docker-сети).
# 3) Гарантировать наличие расширения `pxf` в БД GPDB (иначе внешние таблицы падают с
#    `ERROR: protocol "pxf" does not exist`).
#
# Важно: это учебный стенд, поэтому мы сознательно выбираем простые и надёжные решения
# (например, trust для samehost), а не «боевой» hardened security.

ensure_script="/opt/pxf-scripts/ensure_pxf_bookings.sh"

# PXF CLI использует libpq и при `pxf cluster start` подключается к GPDB как к обычному Postgres.
# После `docker compose stop/start` может внезапно потребоваться пароль (см. ensure_pg_hba_trust ниже),
# поэтому экспортируем PGPASSWORD заранее: это уменьшает "флап" и делает поведение воспроизводимым.
if [ -z "${PGPASSWORD:-}" ]; then
    export PGPASSWORD="${GREENPLUM_PASSWORD:-gpadmin}"
fi

ensure_pg_hba_trust() {
    # После `docker compose stop/start` Docker может выдать контейнеру другой IP.
    # У базового образа Greenplum встречается trust-правило на конкретный /32 (старый IP),
    # и тогда аутентификация по TCP начинает идти через md5 → PXF падает на `28P01`.
    #
    # Решение: добавить правило `host all gpadmin samehost trust`, которое срабатывает для
    # подключений "с этого же контейнера" (PXF запускается рядом с master).
    # Это максимально простое и стабильное правило для учебного стенда.
    local gp_user="${GREENPLUM_USER:-gpadmin}"
    local data_dir="${GREENPLUM_DATA_DIRECTORY:-/data}"
    local attempts=60
    local pg_hba=""
    local trust_line="host all ${gp_user} samehost trust"

    find_pg_hba() {
        local candidate
        for candidate in "${data_dir}/master"/*/pg_hba.conf; do
            if [ -f "${candidate}" ]; then
                echo "${candidate}"
                return 0
            fi
        done
        return 1
    }

    for _ in $(seq 1 "${attempts}"); do
        pg_hba="$(find_pg_hba || true)"
        if [ -n "${pg_hba}" ]; then
            break
        fi
        sleep 2
    done

    if [ -z "${pg_hba}" ]; then
        echo "WARN - pg_hba.conf не найден, пропускаем trust для ${gp_user}"
        return 0
    fi

    if grep -Eq "^[[:space:]]*host[[:space:]]+all[[:space:]]+${gp_user}[[:space:]]+samehost[[:space:]]+trust" "${pg_hba}"; then
        echo "INFO - pg_hba.conf уже содержит trust для ${gp_user} samehost"
        return 0
    fi

    # Вставляем trust-правило перед самым "общим" md5-правилом (0.0.0.0/0),
    # чтобы samehost гарантированно матчился раньше.
    awk -v trust_line="${trust_line}" '
        BEGIN { added = 0 }
        $0 ~ /^[[:space:]]*host[[:space:]]+all[[:space:]]+all[[:space:]]+0\.0\.0\.0\/0[[:space:]]+md5/ && added == 0 {
            print trust_line
            added = 1
        }
        { print }
        END {
            if (added == 0) {
                print trust_line
            }
        }
    ' "${pg_hba}" > "${pg_hba}.tmp" && mv "${pg_hba}.tmp" "${pg_hba}"

    echo "INFO - pg_hba.conf: добавлен trust для ${gp_user} samehost"

    local master_dir="${pg_hba%/pg_hba.conf}"
    # Если GPDB уже поднялся, достаточно reload, чтобы новое правило применилось без рестарта.
    if /usr/local/greenplum-db/bin/pg_ctl -D "${master_dir}" status >/dev/null 2>&1; then
        if ! /usr/local/greenplum-db/bin/pg_ctl -D "${master_dir}" reload >/dev/null 2>&1; then
            echo "WARN - не удалось перезагрузить pg_hba.conf (pg_ctl reload)"
        fi
    fi
}

ensure_pxf_extension() {
    # В базовом /start_gpdb.sh создание `CREATE EXTENSION pxf` зависит от наличия
    # `${PXF_BASE}/conf/pxf-env.sh`. В нашем стенде pxf-env.sh может быть уже создан
    # ensure-скриптом (PXF_BASE на томе), и тогда /start_gpdb.sh пропускает extension.
    #
    # Чтобы внешние таблицы через PXF работали после любого рестарта, создаём extension сами,
    # но только когда GPDB начнёт принимать подключения (с ретраями).
    local gp_user="${GREENPLUM_USER:-gpadmin}"
    local gp_db="${GREENPLUM_DATABASE_NAME:-gp_dwh}"
    local gp_password="${GREENPLUM_PASSWORD:-gpadmin}"
    local attempts=60

    if [ "${GREENPLUM_PXF_ENABLE:-false}" != "true" ]; then
        return 0
    fi

    extension_exists() {
        local result
        # Подключаемся к localhost: это "локальный" путь и на нём обычно уже есть trust.
        result=$(PGPASSWORD="${gp_password}" /usr/local/greenplum-db/bin/psql \
            -h 127.0.0.1 -p 5432 -U "${gp_user}" -d "${gp_db}" \
            -t -A -c "SELECT 1 FROM pg_extension WHERE extname='pxf';" 2>/dev/null || true)
        [ "${result}" = "1" ]
    }

    for attempt in $(seq 1 "${attempts}"); do
        # Ждём, пока master начнёт принимать подключения.
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

# 1) Подготовка PXF_BASE (копирование seed, конфигов и т.д.).
if [ -f "${ensure_script}" ]; then
    "${ensure_script}"
else
    echo "WARN - не найден ensure-скрипт PXF: ${ensure_script}"
fi

# 2) Дальше запускаем две "подстраховки" параллельно, чтобы не замедлять старт контейнера:
# - правка pg_hba.conf (как только он появится на томе);
# - создание extension pxf (как только GPDB начнёт отвечать).
ensure_pg_hba_trust &

ensure_pxf_extension &

# 3) Стартуем GPDB "как обычно". Важно использовать exec, чтобы сигналы Docker
# (stop/restart) корректно приходили в основной процесс entrypoint.
exec /start_gpdb.sh
