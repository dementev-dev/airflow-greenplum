# План: DM-слой (Data Mart) для учебного стенда Bookings

## Context

DWH-стенд уже имеет полностью реализованные слои STG (9 таблиц) -> ODS (9 таблиц, SCD1) -> DDS (6 измерений + 1 факт, Star Schema). DM-слой — финальный аналитический слой, который:
- даёт студентам опыт построения витрин поверх Star Schema;
- демонстрирует реалистичные паттерны (UPSERT, full rebuild, AO Column Store);
- служит основой для будущего Superset-дашборда.

**1 витрина — эталонная** (показывается студенту), **4 остальные — демо/задания**.

---

## 5 витрин DM

### 1. `dm.sales_report` (ЭТАЛОННАЯ)

**Бизнес-вопрос**: "Какова выручка, кол-во билетов и boarding rate по направлениям/тарифам за каждый день?"

**Зерно**: `(flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk)`

**Поля**:
```
-- Ключ
flight_date            DATE NOT NULL
departure_airport_sk   INTEGER NOT NULL
arrival_airport_sk     INTEGER NOT NULL
tariff_sk              INTEGER NOT NULL
-- Денормализованные атрибуты
departure_city         TEXT NOT NULL
departure_airport_bk   TEXT NOT NULL
arrival_city           TEXT NOT NULL
arrival_airport_bk     TEXT NOT NULL
fare_conditions        TEXT NOT NULL
day_of_week            INTEGER NOT NULL
day_name               TEXT NOT NULL
is_weekend             BOOLEAN NOT NULL
-- Метрики
tickets_sold           INTEGER NOT NULL
passengers_boarded     INTEGER NOT NULL
total_revenue          NUMERIC(15,2) NOT NULL
avg_price              NUMERIC(10,2) NOT NULL
min_price              NUMERIC(10,2)
max_price              NUMERIC(10,2)
boarding_rate          NUMERIC(5,4) NOT NULL   -- boarded / sold
-- Служебные
created_at, updated_at, _load_id, _load_ts
```

**Источники**: `fact_flight_sales` JOIN `dim_calendar`, `dim_airports` (x2), `dim_tariffs`

**Загрузка**: инкрементальный UPSERT (UPDATE изменившихся + INSERT новых по ключу)

**Хранение**: `DISTRIBUTED BY (flight_date)`, heap (нужен UPDATE)

**Учит**: денормализация измерений, GROUP BY + агрегация, UPSERT по составному ключу, IS DISTINCT FROM

---

### 2. `dm.route_performance`

**Бизнес-вопрос**: "Какие маршруты самые прибыльные? Каков load factor (заполняемость)?"

**Зерно**: `(route_bk)` — одна строка на бизнес-ключ маршрута

**Поля**:
```
route_bk               TEXT NOT NULL       -- бизнес-ключ (GROUP BY по нему, чтобы учесть все SCD2-версии)
route_sk               INTEGER             -- SK текущей версии (для денормализации)
departure_airport_bk, departure_city       -- из текущей версии dim_routes
arrival_airport_bk, arrival_city
airplane_bk, airplane_model, total_seats   -- из dim_airplanes (через текущую версию маршрута)
-- Метрики
total_flights          INTEGER NOT NULL     -- COUNT(DISTINCT flight_id)
total_tickets          INTEGER NOT NULL
total_boarded          INTEGER NOT NULL
total_revenue          NUMERIC(15,2) NOT NULL
avg_ticket_price       NUMERIC(10,2)
avg_boarding_rate      NUMERIC(5,4) NOT NULL
avg_load_factor        NUMERIC(5,4)         -- AVG(boarded_per_flight / total_seats)
first_flight_date, last_flight_date        DATE
-- Служебные
created_at, updated_at, _load_id, _load_ts
```

**Источники**: `fact_flight_sales` JOIN `dim_routes` (все версии по route_sk), `dim_airports`, `dim_airplanes`, `dim_calendar`

**Нюанс SCD2**: Факт содержит `route_sk`, привязанный к конкретной версии. Агрегируем по `route_bk` (через JOIN dim_routes), чтобы собрать метрики **всех** версий. Атрибуты берём из текущей версии (`valid_to IS NULL`).

**Загрузка**: full rebuild (TRUNCATE + INSERT) — таблица маленькая (~1000 строк)

**Хранение**: `DISTRIBUTED BY (route_bk)`, **AO Column** (нет UPDATE, чисто аналитические чтения — демонстрация отличия от heap)

**Учит**: TRUNCATE + INSERT как альтернатива UPSERT, AO Column Store, load factor, агрегация по SCD2 через route_bk, подзапрос для двухуровневой агрегации

---

### 3. `dm.passenger_loyalty`

**Бизнес-вопрос**: "Кто наши частые пассажиры, сколько тратят, каков их любимый тариф?"

**Зерно**: `(passenger_sk)`

**Поля**:
```
passenger_sk           INTEGER NOT NULL
passenger_bk           TEXT NOT NULL
passenger_name         TEXT NOT NULL
-- Метрики
total_bookings         INTEGER NOT NULL     -- COUNT(DISTINCT book_ref)
total_flights          INTEGER NOT NULL     -- COUNT(*)
total_boarded          INTEGER NOT NULL
total_spent            NUMERIC(15,2) NOT NULL
avg_ticket_price       NUMERIC(10,2)
favorite_tariff        TEXT                 -- самый частый тариф (MODE)
unique_routes          INTEGER NOT NULL     -- COUNT(DISTINCT route_sk)
first_flight_date, last_flight_date        DATE
days_as_customer       INTEGER              -- last - first
-- Служебные
created_at, updated_at, _load_id, _load_ts
```

**Источники**: `fact_flight_sales` JOIN `dim_passengers`, `dim_tariffs`, `dim_calendar`

**Загрузка**: UPSERT (664K пассажиров — full rebuild дорогой)

**Хранение**: `DISTRIBUTED BY (passenger_sk)`, heap

**Учит**: DISTINCT ON / ROW_NUMBER для "самого частого", COUNT(DISTINCT) по нескольким полям, RFM-подобные метрики, UPSERT на большой таблице

---

### 4. `dm.airport_traffic`

**Бизнес-вопрос**: "Каков ежедневный пассажиропоток аэропорта? Сколько вылетов vs прилётов?"

**Зерно**: `(traffic_date, airport_sk)`

**Поля**:
```
traffic_date           DATE NOT NULL
airport_sk             INTEGER NOT NULL
airport_bk             TEXT NOT NULL
airport_name           TEXT NOT NULL
city                   TEXT NOT NULL
-- Метрики вылета
departures_flights     INTEGER NOT NULL DEFAULT 0
departures_passengers  INTEGER NOT NULL DEFAULT 0
departures_revenue     NUMERIC(15,2) NOT NULL DEFAULT 0
-- Метрики прилёта
arrivals_flights       INTEGER NOT NULL DEFAULT 0
arrivals_passengers    INTEGER NOT NULL DEFAULT 0
arrivals_revenue       NUMERIC(15,2) NOT NULL DEFAULT 0
-- Итого
total_passengers       INTEGER NOT NULL     -- departures + arrivals
-- Служебные
created_at, updated_at, _load_id, _load_ts
```

**Источники**: `fact_flight_sales` JOIN `dim_calendar`, `dim_airports` (dual-role: departure + arrival через UNION ALL в CTE)

**Ключевой паттерн**: UNION ALL для "разворота" двух ролей аэропорта:
```sql
WITH traffic AS (
    SELECT cal.date_actual, f.departure_airport_sk AS airport_sk,
           'departure' AS direction, ...
    FROM fact_flight_sales f JOIN dim_calendar cal ...
    UNION ALL
    SELECT cal.date_actual, f.arrival_airport_sk AS airport_sk,
           'arrival' AS direction, ...
    FROM fact_flight_sales f JOIN dim_calendar cal ...
)
SELECT airport_sk, date_actual,
       SUM(CASE WHEN direction='departure' THEN flights END) AS departures_flights, ...
FROM traffic GROUP BY ...
```

**Загрузка**: UPSERT по (traffic_date, airport_sk)

**Хранение**: `DISTRIBUTED BY (traffic_date)`, heap

**Учит**: dual-role dimension join (UNION ALL), conditional aggregation (CASE WHEN + SUM), паттерн "unpivot → aggregate"

---

### 5. `dm.monthly_overview`

**Бизнес-вопрос**: "Каковы помесячные тренды: выручка, пассажиропоток, заполняемость по типам самолётов?"

**Зерно**: `(year_actual, month_actual, airplane_sk)`

**Поля**:
```
year_actual            INTEGER NOT NULL
month_actual           INTEGER NOT NULL
airplane_sk            INTEGER NOT NULL
airplane_bk            TEXT NOT NULL
airplane_model         TEXT NOT NULL
total_seats            INTEGER
-- Метрики
total_flights          INTEGER NOT NULL     -- COUNT(DISTINCT flight_id)
total_tickets          INTEGER NOT NULL
total_boarded          INTEGER NOT NULL
total_revenue          NUMERIC(15,2) NOT NULL
avg_ticket_price       NUMERIC(10,2)
avg_load_factor        NUMERIC(5,4)         -- AVG(boarded_per_flight / total_seats)
unique_routes          INTEGER NOT NULL
unique_passengers      INTEGER NOT NULL
-- Служебные
created_at, updated_at, _load_id, _load_ts
```

**Источники**: `fact_flight_sales` JOIN `dim_calendar`, `dim_airplanes`

**Загрузка**: UPSERT по (year_actual, month_actual, airplane_sk)

**Хранение**: `DISTRIBUTED BY (year_actual)`, heap

**Учит**: двухуровневая агрегация (сначала по рейсу для load factor, потом по месяцу), NULLIF для деления, COUNT(DISTINCT) на нескольких полях, executive-дашборд

---

## DAG-структура

### DAG `bookings_dm_ddl` (DDL)

Линейная цепочка из 5 задач (по аналогии с `bookings_dds_ddl.py`):
```
apply_dm_sales_report_ddl >> apply_dm_route_performance_ddl
>> apply_dm_passenger_loyalty_ddl >> apply_dm_airport_traffic_ddl
>> apply_dm_monthly_overview_ddl
```

### DAG `bookings_to_gp_dm` (ETL + DQ)

Все 5 витрин **параллельно** (читают из DDS, не зависят друг от друга):
```
                  load_dm_sales_report       → dq_dm_sales_report       ─┐
                  load_dm_route_performance   → dq_dm_route_performance   ─┤
start_dm ──>>     load_dm_passenger_loyalty   → dq_dm_passenger_loyalty   ─┤── >> finish_dm_summary
                  load_dm_airport_traffic     → dq_dm_airport_traffic     ─┤
                  load_dm_monthly_overview    → dq_dm_monthly_overview    ─┘
```

Задач: 1 (start) + 5 (load) + 5 (dq) + 1 (finish) = **12 задач**.

---

## Файлы для создания/изменения

### Новые файлы (17 шт.)

**SQL** (`sql/dm/` — 15 файлов):
1. `sql/dm/sales_report_ddl.sql`
2. `sql/dm/sales_report_load.sql`
3. `sql/dm/sales_report_dq.sql`
4. `sql/dm/route_performance_ddl.sql`
5. `sql/dm/route_performance_load.sql`
6. `sql/dm/route_performance_dq.sql`
7. `sql/dm/passenger_loyalty_ddl.sql`
8. `sql/dm/passenger_loyalty_load.sql`
9. `sql/dm/passenger_loyalty_dq.sql`
10. `sql/dm/airport_traffic_ddl.sql`
11. `sql/dm/airport_traffic_load.sql`
12. `sql/dm/airport_traffic_dq.sql`
13. `sql/dm/monthly_overview_ddl.sql`
14. `sql/dm/monthly_overview_load.sql`
15. `sql/dm/monthly_overview_dq.sql`

**DAG** (`airflow/dags/` — 2 файла):
16. `airflow/dags/bookings_dm_ddl.py`
17. `airflow/dags/bookings_to_gp_dm.py`

### Изменяемые файлы (3 шт.)

18. `sql/ddl_gp.sql` — добавить `\i dm/*_ddl.sql` в конец
19. `tests/test_dags_smoke.py` — 2 новых теста (DDL DAG + ETL DAG)
20. `docs/internal/db_schema.md` — добавить DM-слой в описание/Mermaid

### Документация (2 шт.)

21. `docs/internal/bookings_dm_design.md` — полный дизайн-документ DM-слоя (этот файл)
22. `docs/bookings_to_gp_dm.md` — инструкция для студентов (аналог `bookings_to_gp_dds.md`)

---

## Порядок реализации

### Этап 1: Инфраструктура + эталонная витрина `dm.sales_report`
- Дизайн-документ `docs/internal/bookings_dm_design.md`
- DDL + load + DQ для sales_report
- Оба DAG (изначально с 1 витриной)
- Обновить `ddl_gp.sql`
- Smoke-тесты
- Документация для студентов

### Этап 2: `dm.route_performance` (full rebuild + AO Column)
- DDL + load + DQ
- Расширить оба DAG и smoke-тесты

### Этап 3: `dm.passenger_loyalty`
- DDL + load + DQ
- Расширить DAG и тесты

### Этап 4: `dm.airport_traffic` (dual-role dimension)
- DDL + load + DQ
- Расширить DAG и тесты

### Этап 5: `dm.monthly_overview` + финализация
- DDL + load + DQ
- Финализировать DAG и тесты
- Обновить `db_schema.md` (Mermaid lineage, статус)

---

## DQ-проверки (общий паттерн для всех витрин)

PL/pgSQL `DO $$` блоки (как в DDS):
1. Таблица не пуста
2. Нет дублей по составному ключу
3. Бизнес-инварианты (`tickets_sold >= passengers_boarded`, `boarding_rate BETWEEN 0 AND 1`, `total_revenue >= 0`)
4. Обязательные поля не NULL/пустые
5. Для route_performance: `avg_load_factor IS NULL OR avg_load_factor BETWEEN 0 AND 2`

---

## Ключевые образцы для переиспользования

| Что | Файл-образец |
|-----|--------------|
| ETL DAG (PostgresOperator, зависимости) | `airflow/dags/bookings_to_gp_dds.py` |
| DDL DAG (линейная цепочка) | `airflow/dags/bookings_dds_ddl.py` |
| UPSERT SQL (UPDATE + INSERT + CTE) | `sql/dds/fact_flight_sales_load.sql` |
| DQ PL/pgSQL (RAISE EXCEPTION/NOTICE) | `sql/dds/fact_flight_sales_dq.sql` |
| DDL (CREATE TABLE IF NOT EXISTS) | `sql/dds/dim_airports_ddl.sql` |
| Smoke-тесты DAG | `tests/test_dags_smoke.py` |
| Naming conventions | `docs/internal/naming_conventions.md` |

---

## Верификация (end-to-end)

1. `make fmt && make lint` — код проходит проверки
2. `make test` — smoke-тесты DAG зелёные (включая 2 новых)
3. `make ddl-gp` — DDL всех слоёв (STG + ODS + DDS + DM) применяется без ошибок
4. Запустить `bookings_to_gp_dm` в Airflow → все 12 задач зелёные
5. SQL-проверки в Greenplum:
   - `SELECT COUNT(*) FROM dm.sales_report;` — не пусто
   - `SELECT COUNT(*) FROM dm.route_performance;` — ~число текущих маршрутов
   - Нет дублей по составным ключам
   - `boarding_rate BETWEEN 0 AND 1` для всех строк
