# Протокол сквозного (E2E) тестирования ETL

Этот документ описывает процедуру полной проверки цепочки ETL: `STG -> ODS -> DDS -> DM`.
Цель теста — убедиться в корректности инкрементальной загрузки, работы паттерна `Temporary Table` и механизмов `HWM`.

**Основной метод взаимодействия — Airflow UI + REST API.** Запускать пайплайны удобнее всего через веб-интерфейс, а вот отлаживать упавшие задачи (читать логи, очищать статус) полезно уметь через REST API. Это приближает опыт к реальной боевой эксплуатации.

---

## 1. Подготовка окружения

Убедитесь, что все сервисы запущены и генератор инициализирован.

```bash
make up
make bookings-init
```

**Доступ к API:**
В `docker-compose.yml` включена базовая аутентификация (`basic_auth`).
Для curl-запросов используйте учетные данные из вашего `.env` файла (переменные `AIRFLOW_USER` и `AIRFLOW_PASSWORD`, по умолчанию `admin:admin`).

- **UI:** `http://localhost:8080`
- **API Endpoint:** `http://localhost:8080/api/v1`

---

## 2. Очистка данных (Reset)

Перед началом теста необходимо полностью очистить все слои DWH.

```bash
make dwh-truncate
```
*(Если вы меняли DDL, лучше полностью пересоздать схемы: `make gp-psql -c "DROP SCHEMA IF EXISTS ods CASCADE; DROP SCHEMA IF EXISTS dds CASCADE; DROP SCHEMA IF EXISTS dm CASCADE; CREATE SCHEMA ods; CREATE SCHEMA dds; CREATE SCHEMA dm;"`)*

---

## 3. Этап 1: Создание схем и Загрузка за первый день (Initial Load)

Рекомендуется запускать слои последовательно, дожидаясь завершения предыдущего.

### Создание DDL
Откройте **Airflow UI** (`http://localhost:8080`) и нажмите кнопку **▶ Play -> Trigger DAG** для DDL-дагов:
1. `bookings_stg_ddl`
2. `bookings_ods_ddl`
3. `bookings_dds_ddl`
4. `bookings_dm_ddl`

### Запуск пайплайна (Day 1)
После успешного создания таблиц, запустите DAG загрузки для `bookings_to_gp_stage`. 
Откройте **Airflow UI** (`http://localhost:8080`), **снимите DAG с паузы** (переключатель слева от названия) и нажмите кнопку **▶ Play -> Trigger DAG**.
*(Airflow автоматически сгенерирует `logical_date` и `run_id`, например `manual__2026-03-03T10:00:00+00:00`)*.

Либо сделайте то же самое через API (без указания даты). **Важно:** при старте стенда все DAG-и находятся на паузе. Чтобы планировщик начал выполнять запущенный вами DAG, его нужно предварительно "разморозить" (unpause):
```bash
# Снятие с паузы
curl -s -X PATCH "http://localhost:8080/api/v1/dags/bookings_to_gp_stage" \
--user "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" \
-H "Content-Type: application/json" \
-d '{"is_paused": false}'

# Запуск
curl -s -X POST "http://localhost:8080/api/v1/dags/bookings_to_gp_stage/dagRuns" \
--user "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" \
-H "Content-Type: application/json" \
-d '{}'
```

### Проверка статуса
Следите за графом выполнения в UI. Как только DAG перейдет в статус `success`, поочередно запускайте следующие слои:
1. `bookings_to_gp_ods`
2. `bookings_to_gp_dds`
3. `bookings_to_gp_dm`

---

## 4. Этап 2: Проверка инкремента

Эмулируйте появление данных за второй день и проверьте дозагрузку. DAG слоя STG автоматически сгенерирует новый день в базе-источнике перед загрузкой.

```bash
# Повторный запуск цепочки ETL (DAG STG сам сгенерирует новый день)
# Снова нажмите "Trigger DAG" в UI для каждого слоя (STG -> ODS -> DDS -> DM).
```

---

## 5. Цикл отладки: Логи и Перезапуск (Clear)

Если DAG упал, **не нужно пересоздавать стенд с нуля**. Airflow позволяет исправить код и перезапустить только упавшие задачи.

Для выполнения команд ниже вам понадобится **Run ID** упавшего запуска. Его можно скопировать из UI (вкладка *Graph* -> кликнуть на фон сетки -> вкладка *Details* -> `Run ID`) или получить последним API-запросом:

```bash
# Получить Run ID последнего запуска ODS
curl -s "http://localhost:8080/api/v1/dags/bookings_to_gp_ods/dagRuns?order_by=-execution_date&limit=1" \
--user "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" | grep -o '"dag_run_id": "[^"]*"'
```

### Чтение логов через API
Подставьте ваш `<RUN_ID>` (например, `manual__2026-03-03T...`) и имя упавшей таски:
```bash
curl -s "http://localhost:8080/api/v1/dags/bookings_to_gp_ods/dagRuns/<RUN_ID>/taskInstances/<TASK_ID>/logs/1" \
--user "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}"
```

### Перезапуск задачи (Clear)
1. Прочитайте ошибку в логах.
2. Исправьте SQL-файл локально на хосте.
3. Очистите состояние упавших задач (`only_failed: true`) в конкретном запуске, передав ваш `<RUN_ID>`:

```bash
curl -s -X POST "http://localhost:8080/api/v1/dags/bookings_to_gp_ods/clearTaskInstances" \
--user "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" \
-H "Content-Type: application/json" \
-d '{
  "only_failed": true,
  "reset_dag_runs": true,
  "dag_run_id": "<RUN_ID>"
}'
```
После этого планировщик подхватит обновленный SQL-код и продолжит выполнение DAG с точки падения. Вы также можете сделать это в UI: клик по упавшей задаче -> кнопка **Clear**.

---

## 6. Финальная верификация (Критерии успеха)

Выполните SQL-запрос для сверки данных:

```bash
make gp-psql -c "
SELECT 'STG' as layer, COUNT(*) FROM stg.bookings
UNION ALL
SELECT 'ODS' as layer, COUNT(*) FROM ods.bookings
UNION ALL
SELECT 'DDS' as layer, COUNT(*) FROM dds.fact_flight_sales
UNION ALL
SELECT 'DM ' as layer, COUNT(*) FROM dm.sales_report;
"
```

**Критерии корректности:**
1. **Инкремент STG**: Количество строк в `stg.bookings` после Этапа 2 больше, чем после Этапа 1.
2. **Инкремент ODS**: Количество строк в `ods.bookings` выросло. В ODS нет дублей (`SELECT book_ref FROM ods.bookings GROUP BY book_ref HAVING COUNT(*) > 1` должен вернуть 0 строк).
3. **ODS Справочники**: Количество строк в `ods.airports` и `ods.routes` не должно меняться между днями (работает паттерн TRUNCATE+INSERT полного снимка).
4. **HWM в DM**: Витрина `sales_report` содержит данные за оба дня. Значение `COUNT(*)` после Этапа 2 выросло.
5. **Lineage**: Поля `_load_id` и `_load_ts` во всех слоях содержат метки соответствующих запусков (`manual__...`).

---

## 7. Зафиксированный опыт (Типичные ошибки)

- **Рассинхронизация DDL и Load скриптов**: Частая причина падения ODS/DDS — несовпадение имен колонок (например, `amount` vs `segment_amount`) или типов данных (например, `INTEGER[]` vs `TEXT`) между схемой таблицы и запросом загрузки. Внимательно читайте логи задачи.
- **Работа с массивами**: При генерации `hashdiff` в Greenplum/PostgreSQL нельзя использовать пустую строку `''` в `COALESCE` для массива. Массив нужно предварительно привести к тексту: `COALESCE(days_of_week::TEXT, '')`.
- **Кавычки в psql**: При выполнении ручных проверок через `psql -c "..."` помните, что строковые литералы должны оборачиваться в **одинарные кавычки** (`'text'`), а двойные кавычки (`"text"`) интерпретируются как идентификаторы колонок.
