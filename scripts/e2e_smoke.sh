#!/usr/bin/env bash
set -euo pipefail

# Полный smoke-тест стенда: сносит volumes, поднимает стек,
# и проверяет оба учебных DAG через airflow dags test.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

warn() { echo ":: ${1}" >&2; }

wait_up() {
  local svc="$1"
  local attempts=60
  while true; do
    if docker compose -f docker-compose.yml ps "$svc" 2>/dev/null | grep -q "Up"; then
      break
    fi
    attempts=$((attempts - 1))
    if [ "$attempts" -le 0 ]; then
      echo "Service $svc is not up after waiting" >&2
      exit 1
    fi
    sleep 2
  done
}

warn "Reset stack (containers + volumes)"
make clean

warn "Starting stack (docker compose up)"
make up

warn "Waiting for Airflow services"
wait_up airflow-webserver
wait_up airflow-scheduler

warn "Init demo DB bookings"
make bookings-init

warn "Apply DDL to Greenplum"
make ddl-gp

warn "Run local pytest suite"
make test

warn "Airflow DAG test: csv_to_greenplum"
docker compose -f docker-compose.yml exec airflow-webserver airflow dags test csv_to_greenplum 2024-01-01

warn "Airflow DAG test: bookings_to_gp_stage"
docker compose -f docker-compose.yml exec airflow-webserver airflow dags test bookings_to_gp_stage 2024-01-01

warn "Smoke test completed successfully"
