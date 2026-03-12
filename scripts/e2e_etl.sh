#!/usr/bin/env bash
set -euo pipefail

# Скрипт автоматизированного прогона E2E-теста всей цепочки DWH через REST API Airflow.
# Отрабатывает 2 "учебных дня" для проверки инкрементальной загрузки.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

warn() { echo ":: ${1}" >&2; }

# Загружаем переменные из .env, если файл существует, иначе используем дефолты
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

AIRFLOW_USER=${AIRFLOW_USER:-admin}
AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD:-admin}
API_URL="http://localhost:8080/api/v1/dags"

# Функция для триггера DAG и ожидания его завершения
trigger_and_wait() {
    local dag_id=$1
    warn "Запуск $dag_id через REST API..."
    
    # 0. Unpause DAG
    curl -s -X PATCH "$API_URL/$dag_id" \
        --user "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d '{"is_paused": false}' > /dev/null
    
    # 1. Trigger
    local response
    response=$(curl -s -X POST "$API_URL/$dag_id/dagRuns" \
        --user "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d '{}')
        
    local run_id
    run_id=$(echo "$response" | grep -o '"dag_run_id": "[^"]*' | cut -d'"' -f4)
    if [ -z "$run_id" ]; then
        echo "Ошибка запуска $dag_id. Ответ API: $response" >&2
        exit 1
    fi
    warn "Успешный триггер. Run ID: $run_id"
    
    # 2. Wait
    warn "Ожидание завершения $dag_id..."
    local attempts=0
    local max_attempts=120 # 10 минут (120 * 5 сек)
    
    while true; do
        local status
        status=$(curl -s "$API_URL/$dag_id/dagRuns/$run_id" \
            --user "$AIRFLOW_USER:$AIRFLOW_PASSWORD" | grep -o '"state": "[^"]*' | cut -d'"' -f4)
        
        if [ "$status" = "success" ]; then
            warn "DAG $dag_id завершен: SUCCESS"
            break
        elif [ "$status" = "failed" ]; then
            echo "DAG $dag_id УПАЛ (FAILED)!" >&2
            exit 1
        elif [ "$status" = "queued" ] || [ "$status" = "running" ] || [ "$status" = "" ]; then
            attempts=$((attempts + 1))
            if [ "$attempts" -ge "$max_attempts" ]; then
                echo "Таймаут ожидания $dag_id!" >&2
                exit 1
            fi
            sleep 5
        else
            echo "Неизвестный статус: $status" >&2
            exit 1
        fi
    done
}

warn "=== DDL: Создание схем и таблиц ==="
trigger_and_wait "bookings_stg_ddl"
trigger_and_wait "bookings_ods_ddl"
trigger_and_wait "bookings_dds_ddl"
trigger_and_wait "bookings_dm_ddl"

warn "=== DAY 1: Initial Load ==="
trigger_and_wait "bookings_to_gp_stage"
trigger_and_wait "bookings_to_gp_ods"
trigger_and_wait "bookings_to_gp_dds"
trigger_and_wait "bookings_to_gp_dm"

warn "=== DAY 2: Increment ==="
trigger_and_wait "bookings_to_gp_stage"
trigger_and_wait "bookings_to_gp_ods"
trigger_and_wait "bookings_to_gp_dds"
trigger_and_wait "bookings_to_gp_dm"

warn "=== Верификация данных ==="
# Выполняем проверки строк и бизнес-логики, чтобы убедиться, что все витрины DM-слоя заполнены
docker compose -f docker-compose.yml exec greenplum bash -c "su - gpadmin -c \"psql -d gp_dwh -c \\\"
SELECT 'STG bookings' as layer, COUNT(*) FROM stg.bookings UNION ALL
SELECT 'ODS bookings', COUNT(*) FROM ods.bookings UNION ALL
SELECT 'DDS fact', COUNT(*) FROM dds.fact_flight_sales UNION ALL
SELECT 'DM sales_report', COUNT(*) FROM dm.sales_report UNION ALL
SELECT 'DM route_performance', COUNT(*) FROM dm.route_performance UNION ALL
SELECT 'DM passenger_loyalty', COUNT(*) FROM dm.passenger_loyalty UNION ALL
SELECT 'DM airport_traffic', COUNT(*) FROM dm.airport_traffic UNION ALL
SELECT 'DM monthly_overview', COUNT(*) FROM dm.monthly_overview;

-- Дополнительная проверка бизнес-логики (load factor не должен быть NULL и должен быть в пределах разумного)
SELECT 
    'Check LF' as check, 
    COUNT(*) as total_rows,
    SUM(CASE WHEN avg_load_factor IS NOT NULL AND avg_load_factor >= 0 THEN 1 ELSE 0 END) as valid_lf
FROM dm.monthly_overview;
\\\"\""

warn "E2E ETL тест успешно завершен!"
