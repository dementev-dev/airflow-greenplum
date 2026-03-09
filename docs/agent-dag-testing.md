# Тестирование DAG (гайд для AI-агентов)

Этот документ описывает, как агенту взаимодействовать с Airflow (без UI) для точечной проверки и отладки DAG'ов в процессе разработки.

## 0. Состояние среды (Предварительная проверка)

Прежде чем тестировать DAG, убедитесь, что стек работает:
```bash
docker compose ps
```
Если контейнеров нет, поднимите стек: `make up`.
Если исходная база данных пуста (например, после `make clean`), проинициализируйте её: `make bookings-init`.

---

## 1. Быстрая проверка структуры графа (Локально)

При любом изменении Python-кода DAG'а сначала проверьте, что он компилируется и структура графа корректна:
```bash
make test
```
Это запустит smoke-тесты (`tests/test_dags_smoke.py`), которые проверят целостность всех DAG'ов без обращения к базе данных.

---

## 2. Запуск конкретного DAG'а (REST API)

Для тестирования загрузки данных запустите измененный DAG через REST API (базовый URL `http://localhost:8080/api/v1`, креды взять из `.venv`).

**Шаг 2.1. Снять DAG с паузы (при старте стенда все DAG'и на паузе — этот шаг обязателен!):**
```bash
curl -s -X PATCH "http://localhost:8080/api/v1/dags/<dag_id>" \
  -u admin:admin -H "Content-Type: application/json" -d '{"is_paused": false}'
```

**Шаг 2.2. Запустить DAG:**
```bash
curl -s -X POST "http://localhost:8080/api/v1/dags/<dag_id>/dagRuns" \
  -u admin:admin -H "Content-Type: application/json" -d '{}' | jq '{dag_run_id}'
```

**Шаг 2.3. Проверить статус выполнения:**
Используйте `dag_run_id` из предыдущего шага:
```bash
curl -s "http://localhost:8080/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>" \
  -u admin:admin | jq '{state}'
```
Повторяйте запрос, пока `state` не станет `success` или `failed`.

---

## 3. Отладка упавших задач

Если DAG перешел в статус `failed`, найдите упавшую задачу:

**Шаг 3.1. Получить статусы всех задач:**
```bash
curl -s "http://localhost:8080/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>/taskInstances" \
  -u admin:admin | jq '.task_instances[] | {task_id, state}'
```

**Шаг 3.2. Посмотреть логи упавшей задачи (через CLI Airflow):**
REST API отдает логи сложно, поэтому для логов проще использовать `docker compose exec`:
```bash
docker compose exec airflow-webserver airflow tasks logs <dag_id> <task_id> <run_id>
```

*(Совет: ищите в логах слова `ERROR`, `Exception` или вывод SQL-ошибок от PostgresOperator).*

---

## 4. Проверка результата в DWH (Greenplum)

Успешное выполнение DAG'а (зеленый статус) не гарантирует, что данные загрузились правильно (например, если источник был пуст). Проверьте целевые таблицы напрямую:

```bash
docker compose exec greenplum bash -lc "su - gpadmin -c \"/usr/local/greenplum-db/bin/psql -t -A -d gp_dwh -c 'SELECT COUNT(*) FROM <схема>.<таблица>;'\""
```
Убедитесь, что таблица содержит ожидаемое количество строк.

---

## 5. Полный сквозной тест (E2E)

Если вы вносили масштабные изменения, затрагивающие несколько слоев DWH, или меняли DDL таблиц, рекомендуется прогнать полный конвейер (STG -> ODS -> DDS -> DM):
```bash
make e2e-etl
```
Этот скрипт сам запустит все нужные DAG'и в правильном порядке и проверит результаты.
