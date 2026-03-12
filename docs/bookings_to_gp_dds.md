# DAG `bookings_to_gp_dds`: `ods` → `dds` в Greenplum

Этот DAG — учебный пример загрузки аналитического слоя **DDS** (Star Schema) из текущего состояния **ODS**.
Здесь сосредоточены ключевые паттерны аналитического хранилища: SCD1, SCD2 с hashdiff,
point-in-time join, защитные LEFT JOIN для устойчивости к data quality аномалиям.

## Что делает DAG

- Загружает 6 измерений DDS:
  - `dds.dim_calendar` — статическое измерение дат (Full Rebuild);
  - `dds.dim_airports`, `dds.dim_airplanes`, `dds.dim_tariffs`, `dds.dim_passengers` — SCD1 UPSERT;
  - `dds.dim_routes` — **SCD2** с `hashdiff`, `valid_from`, `valid_to` + денормализация.
- Загружает факт `dds.fact_flight_sales` — инкрементальный UPSERT по зерну `(ticket_no, flight_id)`.
- Для каждой таблицы выполняет пару задач `load → dq`.
- Использует `_load_id = {{ run_id }}`. DDS не требует `stg_batch_id`, потому что читает
  текущее состояние ODS.

## Что должно быть готово перед запуском

1) Стенд поднят:

```bash
make up
```

2) STG и ODS уже загружены:

- выполнены DAG-и `bookings_to_gp_stage` и `bookings_to_gp_ods`;
- DDL-объекты созданы (`bookings_dds_ddl` или `make ddl-gp`).

## Как запустить

1) Откройте Airflow UI: http://localhost:8080.
2) Если запускаете DDS впервые — выполните `bookings_dds_ddl`.
3) Запустите `bookings_to_gp_dds`.

## Граф зависимостей

```
load_dds_dim_calendar → dq_dds_dim_calendar
    ├─ load_dds_dim_airports  → dq_dds_dim_airports  ─┐
    │                                                   ├─ load_dds_dim_routes
    ├─ load_dds_dim_airplanes → dq_dds_dim_airplanes ─┘     └─ dq_dds_dim_routes
    │                                                               │
    ├─ load_dds_dim_tariffs   → dq_dds_dim_tariffs                  │
    │                                                               │
    └─ load_dds_dim_passengers → dq_dds_dim_passengers              │
                                                                    │
       все 5 dq_dds_dim_* ─────────────────────────────────────────┘
           └─ load_dds_fact_flight_sales
                └─ dq_dds_fact_flight_sales
                     └─ finish_dds_summary
```

Ключевой момент: `dim_routes` зависит от `dim_airports` и `dim_airplanes` (денормализация),
а факт ждёт завершения **всех** пяти измерений.

## Как это работает внутри (по шагам)

### 1) `load_dds_dim_calendar` → `dq_dds_dim_calendar`

- **SQL:** `sql/dds/dim_calendar_load.sql`, `sql/dds/dim_calendar_dq.sql`
- **Паттерн:** Full Rebuild — каждый запуск пересоздаёт календарь целиком.
  Измерение маленькое и детерминированное, дельту считать нет смысла.

### 2–5) SCD1-измерения (параллельно после calendar)

| # | Задача | SQL-файлы | Что загружает |
|---|--------|-----------|---------------|
| 2 | `load_dds_dim_airports` → `dq_dds_dim_airports` | `sql/dds/dim_airports_load.sql`, `sql/dds/dim_airports_dq.sql` | Аэропорты (код, город, координаты) |
| 3 | `load_dds_dim_airplanes` → `dq_dds_dim_airplanes` | `sql/dds/dim_airplanes_load.sql`, `sql/dds/dim_airplanes_dq.sql` | Самолёты (код, модель, кол-во мест) |
| 4 | `load_dds_dim_tariffs` → `dq_dds_dim_tariffs` | `sql/dds/dim_tariffs_load.sql`, `sql/dds/dim_tariffs_dq.sql` | Тарифы (класс обслуживания) |
| 5 | `load_dds_dim_passengers` → `dq_dds_dim_passengers` | `sql/dds/dim_passengers_load.sql`, `sql/dds/dim_passengers_dq.sql` | Пассажиры (ID, имя, контакты) |

Паттерн загрузки — SCD1 UPSERT: TEMP TABLE → UPDATE (IS DISTINCT FROM) → INSERT.

### 6) `load_dds_dim_routes` → `dq_dds_dim_routes` (SCD2)

- **SQL:** `sql/dds/dim_routes_load.sql`, `sql/dds/dim_routes_dq.sql`
- **Паттерн:** SCD2 — самый нетривиальный паттерн в проекте. Работает в 3 фазы:

**Фаза 1. Hashdiff и закрытие старых версий.**
Скрипт считает MD5-хеш от шести бизнес-атрибутов маршрута (`departure_airport`, `arrival_airport`,
`airplane_code`, `days_of_week`, `departure_time`, `duration`).
Если хеш текущей версии в DDS не совпадает с хешем из ODS — старая версия закрывается
(`valid_to = CURRENT_DATE`). Также закрываются маршруты, исчезнувшие из ODS.

**Фаза 2. Вставка новых версий.**
Для изменённых и совершенно новых маршрутов создаётся новая строка.
`valid_from` выставляется в `1900-01-01` для первой версии маршрута и `CURRENT_DATE` для версии 2+.
Суррогатный ключ (`route_sk`) генерируется через `MAX(route_sk) + ROW_NUMBER()`.

> **Важно:** такая генерация SK безопасна только при `max_active_runs=1` (Airflow гарантирует
> последовательный запуск). В боевых системах используют sequence.

**Фаза 3. Обновление денормализованных атрибутов.**
`dim_routes` хранит денормализованные SCD1-атрибуты из `dim_airports` (города)
и `dim_airplanes` (модель, кол-во мест). Если, например, город переименовали —
фаза 3 обновляет **все** версии маршрута (и текущие, и исторические),
при этом `_load_id` и `_load_ts` не перезаписываются (lineage версий сохраняется).

### 7) `load_dds_fact_flight_sales` → `dq_dds_fact_flight_sales`

- **SQL:** `sql/dds/fact_flight_sales_load.sql`, `sql/dds/fact_flight_sales_dq.sql`
- **Зерно:** `(ticket_no, flight_id)` — один билет на один рейс.
- **Паттерн:** инкрементальный UPSERT.

Три учебных приёма в этом скрипте:

**Защитные LEFT JOIN (defensive coding).**
Все JOIN-ы с измерениями — `LEFT JOIN`. DAG гарантирует, что все измерения загружены
и прошли DQ **до** старта факта (жёсткие зависимости в графе). Поэтому в штатном режиме
NULL SK не возникают. LEFT JOIN здесь — защита от data quality аномалий (например, если
в `ods.routes` появится маршрут с несуществующим аэропортом).

DQ-проверки факта отражают эту логику:
- `passenger_sk` и `tariff_sk` — **запрещены** NULL целиком (0 строк);
- route-related FK (`route_sk`, `airport_sk`, `airplane_sk`) и `calendar_sk` —
  допускается до **1%** NULL (NOTICE-предупреждение), при превышении — EXCEPTION.

> Это **не** паттерн late-arriving dimensions (опаздывающих измерений) в классическом
> понимании: backfill NULL SK при повторном запуске не реализован.
> В боевых системах для этого используют «строку-заглушку» (unknown member, SK = 0)
> и отдельный процесс backfill.

**Два пути lookup для аэропортов и маршрутов.**
Аэропорты (`departure_airport_sk`, `arrival_airport_sk`) разрешаются через `ods.routes` →
`dim_airports`. Аэропорты вылета/прилёта одинаковы во всех версиях маршрута, поэтому
point-in-time логика не нужна — безопасно брать актуальную версию из ODS.

`route_sk` и `airplane_sk` разрешаются через point-in-time join с SCD2 `dim_routes`:

```sql
LEFT JOIN dds.dim_routes AS rte
    ON rte.route_bk = flt.route_no
    AND flt.scheduled_departure::DATE >= rte.valid_from
    AND (rte.valid_to IS NULL OR flt.scheduled_departure::DATE < rte.valid_to)
```

Это гарантирует, что факт привязывается к той версии маршрута, которая была актуальна
на дату рейса.

**UPDATE мутабельных полей.**
UPDATE обновляет только `seat_no`, `price`, `is_boarded` (данные, которые реально
могут измениться — посадка пассажира, корректировка цены). SK измерений не перезаписываются —
они зафиксированы на момент вставки.

### 8) `finish_dds_summary`

Ждёт завершения DQ факта и логирует сводку.

## Как проверить результат

```bash
make gp-psql
```

```sql
SELECT COUNT(*) FROM dds.dim_calendar;
SELECT COUNT(*) FROM dds.dim_routes;
SELECT COUNT(*) FROM dds.fact_flight_sales;

-- Проверка: кол-во строк факта ≈ кол-во строк ODS segments
SELECT
    (SELECT COUNT(*) FROM dds.fact_flight_sales) AS fact_rows,
    (SELECT COUNT(*) FROM ods.segments) AS ods_rows;

-- Проверка SCD2: текущие версии маршрутов (valid_to IS NULL)
SELECT COUNT(*) AS current_versions,
       (SELECT COUNT(*) FROM dds.dim_routes) AS total_versions
FROM dds.dim_routes
WHERE valid_to IS NULL;
```

Ожидаемо: `fact_rows = ods_rows`, `current_versions ≤ total_versions`.

## Типичные ошибки

- `relation "dds..." does not exist`:
  - не применён DDS DDL (`bookings_dds_ddl` или `make ddl-gp`).
- DQ падает на `dim_routes`:
  - проверьте согласованность `ods.routes` (дубли/аномальные версии) и перезапустите DAG.
- DQ падает на `fact_flight_sales` по coverage:
  - проверьте, что ODS DAG завершился успешно без пропуска задач.
