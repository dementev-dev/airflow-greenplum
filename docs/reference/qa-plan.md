# План отладки эталонного пайплайна

> Цель: убедиться, что эталонный вертикальный срез (STG→ODS→DDS→DM) работает
> корректно при разных сценариях. Найти и исправить баги ДО того, как начнём
> готовить ветку main для студентов.
>
> Аудитория документа: AI-агент (Sonnet) или человек, выполняющий отладку.
>
> **Ветка `main`:** на main часть загрузок — заглушки (`SELECT 1;`).
> Пустые таблицы на main: `ods.airplanes`, `ods.seats`,
> `dds.dim_airplanes`, `dds.dim_passengers`, `dds.dim_routes`,
> `dm.route_performance`, `dm.passenger_loyalty`, `dm.airport_traffic`, `dm.monthly_overview`.
> Ожидания ниже написаны для ветки `solution` (полная реализация).
>
> Зависимость: перед запуском этого плана нужно починить bookings-db
> (см. `docs/reference/bookings_db_issues.md`).

---

## Предусловия

```bash
make up                  # поднять стенд
make bookings-init       # инициализировать bookings-db (демо-данные)
```

Все проверки выполняются через `make gp-psql` (psql к Greenplum) и Airflow REST API.
Для запуска DAG из CLI:

```bash
# Запуск DAG и получение run_id
curl -s -u admin:admin -X POST \
  "http://localhost:8080/api/v1/dags/<DAG_ID>/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf":{}}' | jq .dag_run_id

# Проверка статуса
curl -s -u admin:admin \
  "http://localhost:8080/api/v1/dags/<DAG_ID>/dagRuns?order_by=-start_date&limit=1" \
  | jq '.dag_runs[0].state'
```

Перед началом тестов — убедиться, что `make test` проходит локально.

---

## Блок 1: Чистый прогон (Day 1)

**Цель:** убедиться, что пайплайн работает на свежих данных без ошибок.

### 1.1 Сброс и загрузка

```bash
make dwh-truncate        # очистить все таблицы GP
```

Запустить DAG'и в порядке:
1. `bookings_stg_ddl` → дождаться success
2. `bookings_ods_ddl` → дождаться success
3. `bookings_dds_ddl` → дождаться success
4. `bookings_dm_ddl` → дождаться success
5. `bookings_to_gp_stage` → дождаться success
6. `bookings_to_gp_ods` → дождаться success
7. `bookings_to_gp_dds` → дождаться success
8. `bookings_to_gp_dm` → дождаться success

### 1.2 Проверки после Day 1

Все запросы выполнять в `make gp-psql`.

#### A. Непустота всех таблиц

```sql
-- STG: все 9 таблиц не пустые
SELECT 'stg.bookings' AS tbl, COUNT(*) FROM stg.bookings
UNION ALL SELECT 'stg.tickets', COUNT(*) FROM stg.tickets
UNION ALL SELECT 'stg.segments', COUNT(*) FROM stg.segments
UNION ALL SELECT 'stg.flights', COUNT(*) FROM stg.flights
UNION ALL SELECT 'stg.airports', COUNT(*) FROM stg.airports
UNION ALL SELECT 'stg.airplanes', COUNT(*) FROM stg.airplanes
UNION ALL SELECT 'stg.routes', COUNT(*) FROM stg.routes
UNION ALL SELECT 'stg.seats', COUNT(*) FROM stg.seats
UNION ALL SELECT 'stg.boarding_passes', COUNT(*) FROM stg.boarding_passes
ORDER BY 1;

-- ODS: эталонные таблицы не пустые (на main ods.airplanes и ods.seats пусты by design)
SELECT 'ods.bookings' AS tbl, COUNT(*) FROM ods.bookings
UNION ALL SELECT 'ods.tickets', COUNT(*) FROM ods.tickets
UNION ALL SELECT 'ods.segments', COUNT(*) FROM ods.segments
UNION ALL SELECT 'ods.flights', COUNT(*) FROM ods.flights
UNION ALL SELECT 'ods.airports', COUNT(*) FROM ods.airports
UNION ALL SELECT 'ods.airplanes', COUNT(*) FROM ods.airplanes
UNION ALL SELECT 'ods.routes', COUNT(*) FROM ods.routes
UNION ALL SELECT 'ods.seats', COUNT(*) FROM ods.seats
UNION ALL SELECT 'ods.boarding_passes', COUNT(*) FROM ods.boarding_passes
ORDER BY 1;

-- DDS: все 7 таблиц не пустые (на main dim_airplanes, dim_passengers, dim_routes пусты by design)
SELECT 'dds.dim_calendar' AS tbl, COUNT(*) FROM dds.dim_calendar
UNION ALL SELECT 'dds.dim_airports', COUNT(*) FROM dds.dim_airports
UNION ALL SELECT 'dds.dim_airplanes', COUNT(*) FROM dds.dim_airplanes
UNION ALL SELECT 'dds.dim_tariffs', COUNT(*) FROM dds.dim_tariffs
UNION ALL SELECT 'dds.dim_passengers', COUNT(*) FROM dds.dim_passengers
UNION ALL SELECT 'dds.dim_routes', COUNT(*) FROM dds.dim_routes
UNION ALL SELECT 'dds.fact_flight_sales', COUNT(*) FROM dds.fact_flight_sales
ORDER BY 1;

-- DM: все 5 витрин не пустые (на main только sales_report непуста, остальные 4 — заглушки)
SELECT 'dm.sales_report' AS tbl, COUNT(*) FROM dm.sales_report
UNION ALL SELECT 'dm.route_performance', COUNT(*) FROM dm.route_performance
UNION ALL SELECT 'dm.passenger_loyalty', COUNT(*) FROM dm.passenger_loyalty
UNION ALL SELECT 'dm.airport_traffic', COUNT(*) FROM dm.airport_traffic
UNION ALL SELECT 'dm.monthly_overview', COUNT(*) FROM dm.monthly_overview
ORDER BY 1;
```

**Ожидание (solution):** ВСЕ таблицы > 0 строк. Если какая-то пустая — это баг.
**Ожидание (main):** Эталонные таблицы > 0; студенческие (см. список выше) = 0 до реализации заданий.

#### B. Сквозная сверка количеств (STG → ODS)

```sql
-- Инкрементальные таблицы: на Day 1 должно быть ODS = STG
SELECT
    'bookings' AS entity,
    (SELECT COUNT(*) FROM stg.bookings) AS stg_cnt,
    (SELECT COUNT(*) FROM ods.bookings) AS ods_cnt
UNION ALL SELECT 'tickets',
    (SELECT COUNT(*) FROM stg.tickets),
    (SELECT COUNT(*) FROM ods.tickets)
UNION ALL SELECT 'segments',
    (SELECT COUNT(*) FROM stg.segments),
    (SELECT COUNT(*) FROM ods.segments)
UNION ALL SELECT 'flights',
    (SELECT COUNT(*) FROM stg.flights),
    (SELECT COUNT(*) FROM ods.flights)
UNION ALL SELECT 'boarding_passes',
    (SELECT COUNT(*) FROM stg.boarding_passes),
    (SELECT COUNT(*) FROM ods.boarding_passes);
```

**Ожидание Day 1:** `stg_cnt = ods_cnt` для инкрементальных таблиц.

```sql
-- Снапшот-таблицы: ODS = последний батч STG
SELECT
    'airports' AS entity,
    (SELECT COUNT(*) FROM stg.airports
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.airports)) AS stg_cnt,
    (SELECT COUNT(*) FROM ods.airports) AS ods_cnt
UNION ALL SELECT 'airplanes',
    (SELECT COUNT(*) FROM stg.airplanes
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.airplanes)),
    (SELECT COUNT(*) FROM ods.airplanes)
UNION ALL SELECT 'routes',
    (SELECT COUNT(*) FROM stg.routes
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.routes)),
    (SELECT COUNT(*) FROM ods.routes)
UNION ALL SELECT 'seats',
    (SELECT COUNT(*) FROM stg.seats
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.seats)),
    (SELECT COUNT(*) FROM ods.seats);
```

**Ожидание:** `stg_cnt = ods_cnt` для снапшот-таблиц.

#### C. Сквозная сверка количеств (ODS → DDS)

```sql
-- Измерения SCD1: число уникальных бизнес-ключей
SELECT
    'airports' AS entity,
    (SELECT COUNT(DISTINCT airport_code) FROM ods.airports) AS ods_bk,
    (SELECT COUNT(*) FROM dds.dim_airports) AS dds_cnt
UNION ALL SELECT 'airplanes',
    (SELECT COUNT(DISTINCT airplane_code) FROM ods.airplanes),
    (SELECT COUNT(*) FROM dds.dim_airplanes)
UNION ALL SELECT 'tariffs',
    (SELECT COUNT(DISTINCT fare_conditions) FROM ods.segments),
    (SELECT COUNT(*) FROM dds.dim_tariffs)
UNION ALL SELECT 'passengers',
    (SELECT COUNT(DISTINCT passenger_id) FROM ods.tickets),
    (SELECT COUNT(*) FROM dds.dim_passengers);
```

**Ожидание:** `ods_bk = dds_cnt` (на первый прогон, без SCD-истории).

```sql
-- Маршруты SCD2: текущих записей = уникальных бизнес-ключей в ODS
-- Примечание: бизнес-ключ = route_no (не route_no || '-' || validity).
-- Один route_no может иметь несколько периодов validity в ods.routes,
-- но DDS берёт только самый свежий (rn=1 по validity DESC).
SELECT
    (SELECT COUNT(DISTINCT route_no) FROM ods.routes) AS ods_routes,
    (SELECT COUNT(*) FROM dds.dim_routes WHERE valid_to IS NULL) AS dds_current,
    (SELECT COUNT(*) FROM dds.dim_routes) AS dds_total;
```

**Ожидание Day 1:** `ods_routes = dds_current = dds_total` (без истории).

```sql
-- Факт: grain = segments
SELECT
    (SELECT COUNT(*) FROM ods.segments) AS ods_segments,
    (SELECT COUNT(*) FROM dds.fact_flight_sales) AS dds_fact;
```

**Ожидание:** `ods_segments = dds_fact`.

#### D. Сквозная сверка: DDS → DM (бизнес-метрики)

```sql
-- Общая выручка: fact vs sales_report
-- Примечание: колонка называется price (не ticket_price)
SELECT
    (SELECT SUM(price) FROM dds.fact_flight_sales) AS fact_revenue,
    (SELECT SUM(total_revenue) FROM dm.sales_report) AS dm_revenue;
```

**Ожидание:** `fact_revenue = dm_revenue` (или объяснимая разница из-за логики витрины).

```sql
-- Количество уникальных пассажиров: fact vs passenger_loyalty
SELECT
    (SELECT COUNT(DISTINCT passenger_sk) FROM dds.fact_flight_sales
     WHERE passenger_sk IS NOT NULL) AS fact_passengers,
    (SELECT COUNT(*) FROM dm.passenger_loyalty) AS dm_passengers;
```

**Ожидание:** совпадение (или объяснимая разница).

```sql
-- Общее число посадок: fact vs sales_report
-- Примечание: колонка называется is_boarded (не boarding_seq)
SELECT
    (SELECT COUNT(*) FROM dds.fact_flight_sales
     WHERE is_boarded = TRUE) AS fact_boarded,
    (SELECT SUM(passengers_boarded) FROM dm.sales_report) AS dm_boarded;
```

#### E. Целостность суррогатных ключей (FK в DDS и DM)

```sql
SELECT 'orphan_route_sk' AS check_name, COUNT(*) AS orphans
FROM dds.fact_flight_sales f
LEFT JOIN dds.dim_routes r ON f.route_sk = r.route_sk
WHERE f.route_sk IS NOT NULL AND r.route_sk IS NULL

UNION ALL
SELECT 'orphan_airport_sk', COUNT(*)
FROM dm.sales_report sr
LEFT JOIN dds.dim_airports a ON sr.airport_sk = a.airport_sk
WHERE sr.airport_sk IS NOT NULL AND a.airport_sk IS NULL

UNION ALL
SELECT 'orphan_tariff_sk', COUNT(*)
FROM dds.fact_flight_sales f
LEFT JOIN dds.dim_tariffs t ON f.tariff_sk = t.tariff_sk
WHERE f.tariff_sk IS NOT NULL AND t.tariff_sk IS NULL

UNION ALL
SELECT 'orphan_passenger_sk', COUNT(*)
FROM dds.fact_flight_sales f
LEFT JOIN dds.dim_passengers p ON f.passenger_sk = p.passenger_sk
WHERE f.passenger_sk IS NOT NULL AND p.passenger_sk IS NULL

UNION ALL
SELECT 'orphan_calendar_sk', COUNT(*)
FROM dds.fact_flight_sales f
LEFT JOIN dds.dim_calendar c ON f.calendar_sk = c.calendar_sk
WHERE f.calendar_sk IS NOT NULL AND c.calendar_sk IS NULL;
```

**Ожидание:** ВСЕ orphans = 0.

---

## Блок 2: Идемпотентность (повторный Day 1)

**Цель:** повторный прогон ODS/DDS/DM НЕ дублирует данные при неизменённом STG.

**Важно:** `bookings_to_gp_stage` ВСЕГДА генерирует следующий день при запуске
(через `generate_bookings_day`). Тест идемпотентности нужно проводить только для
`bookings_to_gp_ods`, `bookings_to_gp_dds`, `bookings_to_gp_dm` — без повторного
запуска STG. Убедитесь, что все STG-батчи уже обработаны ODS перед тестом:

```sql
SELECT COUNT(*) AS unprocessed
FROM stg.bookings
WHERE _load_ts > (SELECT COALESCE(MAX(_load_ts), '1900-01-01') FROM ods.bookings);
-- Ожидание: 0
```

### 2.1 Зафиксировать counts после Day 1

```sql
SELECT 'ods.bookings' AS tbl, COUNT(*) AS cnt FROM ods.bookings
UNION ALL SELECT 'ods.tickets', COUNT(*) FROM ods.tickets
UNION ALL SELECT 'ods.segments', COUNT(*) FROM ods.segments
UNION ALL SELECT 'ods.flights', COUNT(*) FROM ods.flights
UNION ALL SELECT 'dds.fact_flight_sales', COUNT(*) FROM dds.fact_flight_sales
UNION ALL SELECT 'dds.dim_routes', COUNT(*) FROM dds.dim_routes
UNION ALL SELECT 'dm.sales_report', COUNT(*) FROM dm.sales_report
UNION ALL SELECT 'dm.passenger_loyalty', COUNT(*) FROM dm.passenger_loyalty
ORDER BY 1;
```

Записать результаты.

### 2.2 Повторный прогон (без генерации нового дня!)

Запустить снова: STG → ODS → DDS → DM (4 load-DAG'а).

### 2.3 Проверить, что counts не изменились

Повторить запрос из 2.1 и сравнить.

**Ожидание:**
- Инкрементальные (bookings, tickets, segments, flights, boarding_passes,
  fact_flight_sales) — count НЕ увеличился
- Снапшоты (airports, airplanes, routes, seats) — count тот же
- DM (full rebuild) — count тот же

**Если count вырос — баг идемпотентности.** Записать, в какой таблице и на сколько.

---

## Блок 3: Инкремент (Day 2)

**Цель:** после генерации нового дня инкрементальные таблицы растут,
снапшоты обновляются, витрины обогащаются.

### 3.1 Генерация нового дня

```bash
make bookings-generate-day
```

### 3.2 Запуск пайплайна

Запустить STG → ODS → DDS → DM (4 load-DAG'а).

### 3.3 Проверки после Day 2

#### A. Инкрементальные таблицы выросли

```sql
SELECT 'stg.bookings' AS tbl, COUNT(*) FROM stg.bookings
UNION ALL SELECT 'ods.bookings', COUNT(*) FROM ods.bookings
UNION ALL SELECT 'ods.tickets', COUNT(*) FROM ods.tickets
UNION ALL SELECT 'ods.segments', COUNT(*) FROM ods.segments
UNION ALL SELECT 'dds.fact_flight_sales', COUNT(*) FROM dds.fact_flight_sales
ORDER BY 1;
```

**Ожидание:** все count'ы > Day 1.

#### B. STG хранит оба батча

```sql
SELECT _load_id, COUNT(*) FROM stg.bookings GROUP BY 1 ORDER BY 1;
```

**Ожидание:** 2 разных `_load_id`, оба с данными.

#### C. Снапшоты не дублировались

```sql
SELECT
    'airports' AS entity,
    (SELECT COUNT(*) FROM stg.airports
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.airports)) AS stg_last_batch,
    (SELECT COUNT(*) FROM ods.airports) AS ods_cnt
UNION ALL SELECT 'airplanes',
    (SELECT COUNT(*) FROM stg.airplanes
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.airplanes)),
    (SELECT COUNT(*) FROM ods.airplanes)
UNION ALL SELECT 'routes',
    (SELECT COUNT(*) FROM stg.routes
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.routes)),
    (SELECT COUNT(*) FROM ods.routes)
UNION ALL SELECT 'seats',
    (SELECT COUNT(*) FROM stg.seats
     WHERE _load_id = (SELECT MAX(_load_id) FROM stg.seats)),
    (SELECT COUNT(*) FROM ods.seats);
```

**Ожидание:** `stg_last_batch = ods_cnt`.

#### D. SCD2 dim_routes — версионность

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE valid_to IS NULL) AS current_rows,
    COUNT(*) FILTER (WHERE valid_to IS NOT NULL) AS closed_rows
FROM dds.dim_routes;
```

**Ожидание:** `closed_rows = 0` если справочник маршрутов не менялся.
Если `closed_rows > 0` — проверить, действительно ли атрибуты изменились:

```sql
SELECT route_bk, valid_from, valid_to, hashdiff
FROM dds.dim_routes
WHERE route_bk IN (
    SELECT route_bk FROM dds.dim_routes GROUP BY route_bk HAVING COUNT(*) > 1
)
ORDER BY route_bk, valid_from;
```

#### E. DM после инкремента

```sql
SELECT MIN(flight_date), MAX(flight_date), COUNT(DISTINCT flight_date)
FROM dm.sales_report;
```

**Ожидание:** диапазон дат шире, чем после Day 1.

---

## Блок 4: Многодневный прогон (Days 3-5)

**Цель:** поймать баги, которые проявляются только при накоплении данных.

### 4.1 Цикл

Повторить 3 раза:
```bash
make bookings-generate-day
# Запустить STG → ODS → DDS → DM
```

### 4.2 Проверки после 5 дней

#### A. Монотонный рост инкрементальных таблиц

```sql
SELECT _load_id, COUNT(*) FROM stg.bookings GROUP BY 1 ORDER BY 1;
```

**Ожидание:** 5 строк, все с данными.

#### B. Нет дупликатов в ODS (критически важно!)

```sql
SELECT 'ods.bookings' AS tbl,
    COUNT(*) - COUNT(DISTINCT book_ref) AS dups FROM ods.bookings
UNION ALL SELECT 'ods.tickets',
    COUNT(*) - COUNT(DISTINCT ticket_no) FROM ods.tickets
UNION ALL SELECT 'ods.flights',
    COUNT(*) - COUNT(DISTINCT flight_id) FROM ods.flights
UNION ALL SELECT 'ods.segments',
    COUNT(*) - COUNT(DISTINCT ticket_no || '-' || flight_id::text) FROM ods.segments
UNION ALL SELECT 'ods.boarding_passes',
    COUNT(*) - COUNT(DISTINCT ticket_no || '-' || flight_id::text) FROM ods.boarding_passes;
```

**Ожидание:** ВСЕ dups = 0. Если > 0 — **критический баг**.

#### C. Нет дупов в DDS fact

```sql
SELECT COUNT(*) - COUNT(DISTINCT ticket_no || '-' || flight_id::text) AS dups
FROM dds.fact_flight_sales;
```

**Ожидание:** 0.

#### D. Рост витрин осмысленный

```sql
SELECT
    (SELECT COUNT(*) FROM dm.sales_report) AS sales_rows,
    (SELECT COUNT(*) FROM dm.route_performance) AS route_rows,
    (SELECT COUNT(*) FROM dm.passenger_loyalty) AS passenger_rows,
    (SELECT COUNT(*) FROM dm.airport_traffic) AS traffic_rows,
    (SELECT COUNT(*) FROM dm.monthly_overview) AS monthly_rows;
```

Сравнить с Day 1. Ожидание: `sales_rows` и `traffic_rows` растут (по дням),
`route_rows` стабильны (по маршрутам), `passenger_rows` растут или стабильны.

---

## Блок 5: Валидация SQL-скриптов (статический анализ)

**Цель:** проверить качество кода без запуска стенда. Можно делать параллельно
с блоками 1-4.

### 5.1 Консистентность _load_id во всех слоях

```bash
# В STG/ODS/DDS/DM должен быть _load_id
grep -r '_load_id' sql/stg/*_load.sql sql/ods/*_load.sql sql/dds/*_load.sql sql/dm/*_load.sql | head -20

# Не должно быть старых имён batch_id, load_dttm, src_created_at_ts
grep -r '\bbatch_id\b' sql/stg/ sql/ods/ sql/dds/ sql/dm/  # ожидание: только допустимые переменные PL (v_batch_id)
grep -r 'load_dttm\|src_created_at_ts' sql/                 # ожидание: пусто
```

### 5.2 Все load.sql используют шаблон {{ run_id }}

Примечание: ODS намеренно не использует `{{ run_id }}` — вместо этого в `_load_id`
сохраняется `_load_id` из STG для сквозного lineage (traceable to source batch).
Это правильный паттерн, а не баг. Проверять нужно только STG/DDS/DM.

```bash
for f in sql/stg/*_load.sql sql/dds/*_load.sql sql/dm/*_load.sql; do
    if ! grep -q '{{ run_id }}\|{{ ti.xcom_pull' "$f"; then
        echo "WARN: $f не содержит {{ run_id }}"
    fi
done
```

### 5.3 DDL и load совпадают по набору колонок

Для каждой пары `_ddl.sql` / `_load.sql`:
- Извлечь список колонок из DDL (CREATE TABLE)
- Извлечь список колонок из INSERT в load.sql
- Сравнить

**Ожидание:** списки совпадают (за исключением SERIAL/GENERATED колонок).

Приоритетные пары для ручной проверки (сложные):
- `dds/fact_flight_sales` (много FK)
- `dds/dim_routes` (SCD2-поля)
- `dm/sales_report` (агрегаты)

### 5.4 DQ-скрипты согласованы с DDL

Для каждого `_dq.sql` проверить:
- Все NOT NULL колонки из DDL проверяются в DQ?
- Все бизнес-ключи из DDL проверяются на дупликаты?
- FK-проверки ссылаются на правильные таблицы?

---

## Блок 6: Граничные случаи

### 6.1 Пустой инкремент

Запустить STG DAG БЕЗ предварительной генерации нового дня.

**Ожидание:** DAG завершается success (не failure). Таблицы не меняются.
DQ-скрипты не падают на пустом батче.

### 6.2 DDL DAG на уже существующих таблицах

Запустить `bookings_stg_ddl` повторно (таблицы уже есть).

**Ожидание:** success. DDL использует `IF NOT EXISTS`. Данные не потеряны.

### 6.3 Служебные поля заполнены

```sql
SELECT 'ods.bookings' AS tbl,
    COUNT(*) FILTER (WHERE _load_id IS NULL) AS null_load_id,
    COUNT(*) FILTER (WHERE _load_ts IS NULL) AS null_load_ts
FROM ods.bookings
UNION ALL SELECT 'dds.fact_flight_sales',
    COUNT(*) FILTER (WHERE _load_id IS NULL),
    COUNT(*) FILTER (WHERE _load_ts IS NULL)
FROM dds.fact_flight_sales
UNION ALL SELECT 'dds.dim_routes',
    COUNT(*) FILTER (WHERE _load_id IS NULL),
    COUNT(*) FILTER (WHERE _load_ts IS NULL)
FROM dds.dim_routes;
```

**Ожидание:** все null_* = 0.

---

## Формат отчёта

По каждому блоку фиксировать:

| Блок | Проверка | Статус | Детали |
|------|----------|--------|--------|
| 1.2A | Непустота таблиц | OK / FAIL | какая таблица пуста |
| 1.2B | STG→ODS counts | OK / FAIL | расхождение: X vs Y |
| ... | ... | ... | ... |

Если найден баг:
1. Описать симптом (какой запрос, какой результат)
2. Локализовать (в каком SQL-файле проблема)
3. Предложить fix
4. После fix — повторить проверку

---

## Приоритеты (если время ограничено)

1. **Блок 1** (чистый прогон) — обязательно, базовый smoke
2. **Блок 2** (идемпотентность) — обязательно, частый источник багов
3. **Блок 3** (инкремент Day 2) — обязательно, проверяет главную фичу
4. **Блок 5.3** (DDL ↔ load) — высокий приоритет, ловит рассинхрон колонок
5. **Блок 4** (5 дней) — средний приоритет, ловит накопительные баги
6. **Блок 6** (граничные) — средний приоритет
7. **Блок 5.1-5.4** (статика) — можно делать параллельно без стенда
