# Тестирование DAG (гайд для AI-агентов)

Как программно проверить, что DAG работает корректно.
Все проверки выполняются через CLI, `docker compose exec` и SQL-запросы.
Браузер и Airflow UI **не используются**.

---

## Предварительные условия

Перед тестированием DAG стек должен быть поднят и здоров.

```bash
# 1. Поднять стек (если не поднят)
make up

# 2. Дождаться healthy-статуса всех сервисов
docker compose ps          # greenplum и airflow-webserver должны быть (healthy)

# 3. Для DAG bookings_to_gp_stage — инициализировать данные
make bookings-init         # создать демо-БД bookings в контейнере bookings-db
make ddl-gp                # создать STG-слой и внешние PXF-таблицы в Greenplum
```

Если стек ранее сносился (`make clean`), шаги 1-3 обязательны.

---

## Уровень 1. Локальные проверки (без Docker)

Быстрые проверки, не требующие поднятого стека:

```bash
make test          # pytest: unit-тесты helpers + smoke-тесты структуры DAG
make lint          # black + isort в режиме проверки
```

Smoke-тесты DAG (`tests/test_dags_smoke.py`) проверяют:
- DAG импортируется без ошибок;
- все ожидаемые `task_id` присутствуют;
- прямые рёбра графа совпадают с эталонными;
- задачи достижимы друг из друга (транзитивно).

Если Airflow не установлен в venv, smoke-тесты автоматически пропускаются (`skip`).

---

## Уровень 2. Тестовый прогон DAG (без записи в мета-БД)

Команда `airflow dags test` выполняет DAG целиком в оффлайн-режиме
(результат не сохраняется в Airflow, не создаётся `dag_run`):

```bash
docker compose exec airflow-webserver \
  airflow dags test csv_to_greenplum 2024-01-01

docker compose exec airflow-webserver \
  airflow dags test bookings_to_gp_stage 2024-01-01
```

Вывод идёт прямо в stdout — можно парсить на наличие `ERROR` / `FAILED`.

---

## Уровень 3. Полноценный запуск DAG (с записью в мета-БД)

### Запуск

```bash
docker compose exec airflow-webserver \
  airflow dags trigger bookings_to_gp_stage
```

Команда возвращает `run_id`. Если нужно получить его программно:

```bash
docker compose exec airflow-webserver \
  airflow dags list-runs -d bookings_to_gp_stage -o json
```

### Ожидание завершения

DAG может работать 30-60 секунд. Опрашиваем статус задач:

```bash
docker compose exec airflow-webserver \
  airflow tasks states-for-dag-run bookings_to_gp_stage <run_id> -o json
```

Повторять до тех пор, пока все задачи не перейдут в терминальный статус
(`success`, `failed`, `upstream_failed`, `skipped`).

### Проверка результатов

```bash
# Список задач и их статусы (текстовый формат)
docker compose exec airflow-webserver \
  airflow tasks states-for-dag-run bookings_to_gp_stage <run_id>

# Логи конкретной задачи (при отладке)
docker compose exec airflow-webserver \
  airflow tasks logs bookings_to_gp_stage load_airports_to_stg <run_id>
```

**Критерий успеха:** все 20 задач в статусе `success`.

---

## Проверка параллельности

В DAG `bookings_to_gp_stage` задачи `load_airports_to_stg` и `load_airplanes_to_stg`
должны запускаться параллельно (обе зависят только от `check_tickets_dq`).

### Способ 1. По временным меткам (после реального запуска)

```bash
docker compose exec airflow-webserver \
  airflow tasks states-for-dag-run bookings_to_gp_stage <run_id> -o json
```

Сравнить `start_date` задач `load_airports_to_stg` и `load_airplanes_to_stg`.
**Критерий:** разница < 1 секунды.

### Способ 2. По структуре графа (без запуска DAG)

```bash
docker compose exec airflow-webserver python3 -c "
from airflow.models import DagBag

dag = DagBag('/opt/airflow/dags').get_dag('bookings_to_gp_stage')

airports = dag.get_task('load_airports_to_stg')
airplanes = dag.get_task('load_airplanes_to_stg')

# Параллельность: задачи не зависят друг от друга
a_up = {t.task_id for t in airports.upstream_list}
b_up = {t.task_id for t in airplanes.upstream_list}

print('airports upstream:', a_up)
print('airplanes upstream:', b_up)

# airports не должен быть в upstream airplanes и наоборот
assert 'load_airports_to_stg' not in b_up, 'airplanes зависит от airports!'
assert 'load_airplanes_to_stg' not in a_up, 'airports зависит от airplanes!'
print('OK: задачи независимы, могут идти параллельно')
"
```

### Ожидаемые зависимости (эталон)

| Задача | Ждёт (upstream) |
|--------|-----------------|
| `load_airports_to_stg` | `check_tickets_dq` |
| `load_airplanes_to_stg` | `check_tickets_dq` |
| `load_routes_to_stg` | `check_airports_dq` + `check_airplanes_dq` |
| `load_seats_to_stg` | `check_airplanes_dq` |
| `finish_summary` | `check_boarding_passes_dq` + `check_seats_dq` |

---

## Проверка данных в Greenplum

После успешного прогона DAG можно проверить наличие данных напрямую в БД:

```bash
# Количество строк в ключевых таблицах
docker compose exec greenplum bash -lc \
  "su - gpadmin -c \"/usr/local/greenplum-db/bin/psql -t -A -d gp_dwh -c 'SELECT COUNT(*) FROM stg.bookings;'\""

docker compose exec greenplum bash -lc \
  "su - gpadmin -c \"/usr/local/greenplum-db/bin/psql -t -A -d gp_dwh -c 'SELECT COUNT(*) FROM stg.tickets;'\""

docker compose exec greenplum bash -lc \
  "su - gpadmin -c \"/usr/local/greenplum-db/bin/psql -t -A -d gp_dwh -c 'SELECT COUNT(*) FROM stg.airports;'\""
```

**Критерий:** все таблицы непустые (COUNT > 0).

---

## Проверка Airflow Connections

Перед запуском DAG полезно убедиться, что подключения настроены:

```bash
docker compose exec airflow-webserver airflow connections get greenplum_conn
docker compose exec airflow-webserver airflow connections get bookings_db
```

Обе команды должны вернуть параметры подключения без ошибок.

---

## REST API (альтернатива CLI)

REST API удобнее CLI для агента в ряде случаев: не нужен `docker exec`,
возвращает чистый JSON, проще поллить статус в цикле.

**База:** `http://localhost:8080/api/v2` (порт из `AIRFLOW_WEB_PORT`, default: 8080)
**Аутентификация:** HTTP Basic Auth — `AIRFLOW_USER`/`AIRFLOW_PASSWORD` из `.env` (default: `admin`/`admin`)

### Список DAG

```bash
curl -s -u admin:admin http://localhost:8080/api/v2/dags | jq '.dags[].dag_id'
```

### Запуск DAG

```bash
curl -s -u admin:admin \
  -X POST http://localhost:8080/api/v2/dags/bookings_to_gp_stage/dagRuns \
  -H "Content-Type: application/json" \
  -d '{}' | jq '{dag_run_id, state}'
```

Вернёт `dag_run_id` — он нужен для всех последующих запросов.

### Статус запуска DAG

```bash
curl -s -u admin:admin \
  http://localhost:8080/api/v2/dags/bookings_to_gp_stage/dagRuns/<dag_run_id> \
  | jq '{state, start_date, end_date}'
```

Значения `state`: `queued` → `running` → `success` / `failed`.

### Статусы всех задач запуска

```bash
curl -s -u admin:admin \
  "http://localhost:8080/api/v2/dags/bookings_to_gp_stage/dagRuns/<dag_run_id>/taskInstances" \
  | jq '.task_instances[] | {task_id, state, start_date}'
```

### Детали конкретной задачи

```bash
curl -s -u admin:admin \
  "http://localhost:8080/api/v2/dags/bookings_to_gp_stage/dagRuns/<dag_run_id>/taskInstances/load_airports_to_stg" \
  | jq '{task_id, state, start_date, end_date, duration}'
```

### Последний `dag_run_id` без явного сохранения

```bash
curl -s -u admin:admin \
  "http://localhost:8080/api/v2/dags/bookings_to_gp_stage/dagRuns?order_by=-start_date&limit=1" \
  | jq -r '.dag_runs[0].dag_run_id'
```

### Когда использовать REST API вместо CLI

| Ситуация | Предпочтительный способ |
|----------|------------------------|
| Нужен чистый JSON для парсинга | REST API |
| Агент работает вне Docker-хоста | REST API |
| Поллинг статуса в цикле | REST API (проще, чем `exec`) |
| Быстрая отладка или разовая проверка | CLI (`airflow dags test`) |
| Тестовый прогон без записи в мета-БД | CLI (`airflow dags test`) |

---

## Полный E2E-тест (автоматизированный)

Скрипт `scripts/e2e_smoke.sh` выполняет полный цикл:

1. `make clean` — полный reset стека;
2. `make up` — поднимает сервисы;
3. ждёт `airflow-webserver` и `airflow-scheduler`;
4. `make bookings-init` — инициализирует демо-БД;
5. `make ddl-gp` — применяет DDL;
6. `make test` — локальные тесты;
7. `airflow dags test csv_to_greenplum 2024-01-01` — тест CSV-пайплайна;
8. проверяет `public.orders` непустую;
9. `airflow dags test bookings_to_gp_stage 2024-01-01` — тест bookings-пайплайна;
10. проверяет `stg.bookings` непустую.

Запуск:

```bash
./scripts/e2e_smoke.sh
```

---

## Список DAG и ожидаемые задачи

### `csv_to_greenplum` (4 задачи)

`create_orders_table` → `generate_csv` → `preview_csv` → `load_csv_to_greenplum`

### `csv_to_greenplum_dq` (5 задач)

`check_orders_table_exists` → `check_orders_schema` → `check_orders_has_rows`
→ `check_order_duplicates` → `data_quality_summary`

### `bookings_to_gp_stage` (20 задач)

```
generate_bookings_day → load_bookings → check_bookings_dq
    → load_tickets → check_tickets_dq
        ├─ load_airports → check_airports_dq ─┐
        │                                      ├─ load_routes → check_routes_dq
        ├─ load_airplanes → check_airplanes_dq ┤       → load_flights → check_flights_dq
        │                                      │           → load_segments → check_segments_dq
        │                                      │               → load_boarding_passes → check_bp_dq ─┐
        │                                      └─ load_seats → check_seats_dq ──────────────────────┤
        │                                                                                            ▼
        └──────────────────────────────────────────────────────────────── finish_summary
```

---

## Ключевые команды (шпаргалка)

| Действие | Команда |
|----------|---------|
| Список DAG | `docker compose exec airflow-webserver airflow dags list` |
| Список задач DAG | `docker compose exec airflow-webserver airflow tasks list <dag_id>` |
| Тестовый прогон | `docker compose exec airflow-webserver airflow dags test <dag_id> 2024-01-01` |
| Запуск DAG | `docker compose exec airflow-webserver airflow dags trigger <dag_id>` |
| Список запусков | `docker compose exec airflow-webserver airflow dags list-runs -d <dag_id> -o json` |
| Статусы задач | `docker compose exec airflow-webserver airflow tasks states-for-dag-run <dag_id> <run_id>` |
| Логи задачи | `docker compose exec airflow-webserver airflow tasks logs <dag_id> <task_id> <run_id>` |
| Проверка подключений | `docker compose exec airflow-webserver airflow connections get <conn_id>` |
| Запрос в Greenplum | `docker compose exec greenplum bash -lc "su - gpadmin -c '/usr/local/greenplum-db/bin/psql -t -A -d gp_dwh -c \"<SQL>\"'"` |
| Здоровье стека | `docker compose ps` |

---

## Типичные проблемы

| Симптом | Вероятная причина | Что делать |
|---------|-------------------|------------|
| DAG не найден в `dags list` | Синтаксическая ошибка в файле | Посмотреть `docker compose logs airflow-scheduler` |
| `upstream_failed` у задачи | Упала задача выше по графу | Найти первую `failed`-задачу и смотреть её логи |
| DQ-проверка падает | Нет данных в source или нарушена целостность | Проверить данные в `bookings-db` и `stg.*` |
| `Connection ... not found` | Не задана переменная `AIRFLOW_CONN_*` | Проверить `.env` и `docker-compose.yml` |
| Greenplum `unhealthy` | PXF не стартовал (долгая инициализация) | Подождать 2-3 минуты, проверить `docker compose ps` |
| `relation ... does not exist` | Не применён DDL | Выполнить `make ddl-gp` |
| Пустые таблицы stg | Не выполнен `make bookings-init` | Выполнить `make bookings-init`, затем перезапустить DAG |
