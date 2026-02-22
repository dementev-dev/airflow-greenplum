# ODS Layer: план реализации

## Контекст

STG-слой уже реализован как учебный эталон: сырые данные из bookings-db грузятся через PXF, хранятся как TEXT, контролируются batch_id.

**Следующий шаг** — ODS (Operational Data Store): типизированный, очищенный и исторически отслеживаемый слой.

**Образовательные цели** реализации:
1. Показать переход TEXT → правильные типы данных
2. Объяснить SCD Type 2 на конкретных SQL-примерах
3. Показать UPSERT-паттерн для транзакционных данных
4. Продемонстрировать DQ-проверки уровня ODS (типы, ссылочная целостность)
5. Показать параллельный граф DAG с правильными зависимостями

---

## Архитектура ODS

### Отличие ODS от STG

| Аспект | STG | ODS |
|--------|-----|-----|
| Типы | Всё TEXT | Правильные типы (TIMESTAMP, NUMERIC и т.д.) |
| Дедупликация | Нет (все инкременты хранятся) | Одна активная запись на бизнес-ключ |
| История | Нет | SCD Type 2 для справочников |
| Хранилище | appendonly | heap (справочники) + appendonly (транзакции) |
| DQ-проверки | Количество строк, NOT NULL по TEXT | Типы, ссылочная целостность, бизнес-правила |

### Паттерны загрузки

**Справочники (airports, airplanes, routes, seats) — SCD Type 2:**
- Читаем актуальный снапшот из STG (последний batch)
- Закрываем старые версии при изменении атрибутов (is_active=false, dw_end_date)
- Вставляем новые версии (dw_version++, dw_start_date=CURRENT_DATE)
- Вставляем новые записи (dw_version=1)
- Хранилище: heap-таблицы (без appendonly), т.к. UPDATE-операции частые

**Транзакции (bookings, tickets, flights, segments, boarding_passes) — UPSERT:**
- Обновляем изменившиеся записи (UPDATE)
- Вставляем новые записи (INSERT WHERE NOT EXISTS)
- Хранилище: appendonly (как в STG)

---

## Схема ODS-таблиц

### Справочники (с SCD Type 2)

#### ods.airports
```sql
airport_code  TEXT NOT NULL           -- бизнес-ключ
airport_name  TEXT NOT NULL           -- правильный тип (=TEXT, из JSONB в источнике)
city          TEXT NOT NULL
country       TEXT NOT NULL
coordinates   TEXT                    -- оставляем TEXT (сложный формат, DDS распарсит)
timezone      TEXT NOT NULL
-- SCD Type 2
dw_start_date DATE NOT NULL DEFAULT CURRENT_DATE
dw_end_date   DATE                    -- NULL = активная запись
is_active     BOOLEAN NOT NULL DEFAULT true
dw_version    INTEGER NOT NULL DEFAULT 1
-- Технические
load_dttm     TIMESTAMP DEFAULT now()
batch_id      TEXT
DISTRIBUTED BY (airport_code)        -- heap, для эффективных UPDATE
```

#### ods.airplanes
```sql
airplane_code TEXT NOT NULL           -- бизнес-ключ
model         TEXT NOT NULL           -- из JSONB в источнике
range         INTEGER                 -- TEXT → INTEGER (км)
speed         INTEGER                 -- TEXT → INTEGER (км/ч)
-- SCD Type 2 + технические (аналогично airports)
DISTRIBUTED BY (airplane_code)
```

#### ods.routes
```sql
route_no          TEXT NOT NULL       -- бизнес-ключ
validity          TEXT                -- TSTZRANGE → TEXT (сложно парсить, документируем)
departure_airport TEXT NOT NULL       -- FK к ods.airports.airport_code
arrival_airport   TEXT NOT NULL
airplane_code     TEXT NOT NULL       -- FK к ods.airplanes.airplane_code
days_of_week      TEXT                -- int[] → TEXT (объясняем ограничение STG/PXF)
scheduled_time    TIME                -- TEXT → TIME (пример кастинга)
duration          INTERVAL            -- TEXT → INTERVAL (ключевой пример кастинга)
-- SCD Type 2 + технические
DISTRIBUTED BY (route_no)
```

#### ods.seats
```sql
airplane_code TEXT NOT NULL           -- бизнес-ключ (совместный)
seat_no       TEXT NOT NULL           -- бизнес-ключ (совместный)
fare_conditions TEXT NOT NULL         -- Economy/Comfort/Business
-- SCD Type 2 + технические
DISTRIBUTED BY (airplane_code)
```

### Транзакционные (UPSERT)

#### ods.bookings
```sql
book_ref     TEXT NOT NULL            -- бизнес-ключ (CHAR(6))
book_date    TIMESTAMP WITH TIME ZONE -- TEXT → TIMESTAMPTZ (ключевой пример)
total_amount NUMERIC(10,2) NOT NULL   -- TEXT → NUMERIC
-- Технические
src_created_at_ts TIMESTAMP
load_dttm    TIMESTAMP DEFAULT now()
batch_id     TEXT
DISTRIBUTED BY (book_ref)            -- appendonly, выравнивание со STG
```

#### ods.tickets
```sql
ticket_no      TEXT NOT NULL          -- бизнес-ключ
book_ref       TEXT NOT NULL          -- FK к ods.bookings
passenger_id   TEXT NOT NULL
passenger_name TEXT NOT NULL
outbound       BOOLEAN                -- TEXT → BOOLEAN (пример нетривиального кастинга)
-- Технические (src_created_at_ts, load_dttm, batch_id)
DISTRIBUTED BY (book_ref)            -- совместно с bookings
```

#### ods.flights
```sql
flight_id           INTEGER NOT NULL  -- TEXT → INTEGER (бизнес-ключ)
route_no            TEXT NOT NULL     -- FK к ods.routes
status              TEXT NOT NULL
scheduled_departure TIMESTAMP WITH TIME ZONE
scheduled_arrival   TIMESTAMP WITH TIME ZONE
actual_departure    TIMESTAMP WITH TIME ZONE
actual_arrival      TIMESTAMP WITH TIME ZONE
-- Технические (src_created_at_ts, load_dttm, batch_id)
DISTRIBUTED BY (flight_id)
```

#### ods.segments
```sql
ticket_no       TEXT NOT NULL         -- бизнес-ключ (совместный), FK к ods.tickets
flight_id       INTEGER NOT NULL      -- TEXT → INTEGER, FK к ods.flights
fare_conditions TEXT NOT NULL
price           NUMERIC(10,2)         -- TEXT → NUMERIC (в STG называется 'price', не 'amount')
-- Технические
DISTRIBUTED BY (ticket_no)           -- совместно с boarding_passes
```

#### ods.boarding_passes
```sql
ticket_no     TEXT NOT NULL           -- бизнес-ключ, FK к ods.tickets
flight_id     INTEGER NOT NULL        -- TEXT → INTEGER, FK к ods.flights
seat_no       TEXT NOT NULL
boarding_no   INTEGER                 -- TEXT → INTEGER
boarding_time TIMESTAMP WITH TIME ZONE -- TEXT → TIMESTAMPTZ
-- Технические
DISTRIBUTED BY (ticket_no)           -- совместно с segments
```

---

## Структура файлов

```
sql/ods/
├── airports_ddl.sql        ← SCD Type 2 схема + DISTRIBUTED BY
├── airports_load.sql       ← UPDATE (закрыть) + INSERT (новые/изменённые)
├── airports_dq.sql         ← нет дублей is_active, все STG-ключи в ODS
├── airplanes_ddl.sql
├── airplanes_load.sql
├── airplanes_dq.sql
├── routes_ddl.sql          ← обратить внимание: scheduled_time TIME, duration INTERVAL
├── routes_load.sql
├── routes_dq.sql
├── seats_ddl.sql
├── seats_load.sql
├── seats_dq.sql
├── bookings_ddl.sql        ← TIMESTAMPTZ, NUMERIC
├── bookings_load.sql       ← UPDATE изменений + INSERT новых
├── bookings_dq.sql
├── tickets_ddl.sql         ← BOOLEAN для outbound
├── tickets_load.sql
├── tickets_dq.sql
├── flights_ddl.sql         ← INTEGER для flight_id
├── flights_load.sql
├── flights_dq.sql
├── segments_ddl.sql
├── segments_load.sql
├── segments_dq.sql
├── boarding_passes_ddl.sql
├── boarding_passes_load.sql
└── boarding_passes_dq.sql

airflow/dags/
├── bookings_ods_ddl.py     ← аналог bookings_stg_ddl.py (9 PostgresOperator)
└── bookings_to_gp_ods.py   ← аналог bookings_to_gp_stage.py (параллельный граф)

sql/ddl_gp_ods.sql          ← мастер-DDL для make ddl-gp-ods (\i на каждый *_ddl.sql)

docs/bookings_to_gp_ods.md  ← описание DAG + примеры DQ-запросов для проверки
Makefile                    ← + таргет ddl-gp-ods
tests/test_dags_smoke.py    ← + smoke-тесты для двух новых DAG
```

---

## DAG-граф bookings_to_gp_ods (параллельный)

```
load_ods_airports  → dq_ods_airports  ─┐
                                        ├─ load_ods_routes → dq_ods_routes → load_ods_flights → dq_ods_flights ─┐
load_ods_airplanes → dq_ods_airplanes ─┘                                                                         │
                    └─ load_ods_seats → dq_ods_seats                                                             │
                                                                                                                  ↓
load_ods_bookings  → dq_ods_bookings → load_ods_tickets → dq_ods_tickets ──────────────────── load_ods_segments → dq_ods_segments
                                                                                                        ↓
                                                                                         load_ods_boarding_passes → dq_ods_boarding_passes

Все ветки → finish_ods_summary
```

Зависимости:
- `routes` — после `airports` и `airplanes`
- `seats` — после `airplanes`
- `flights` — после `routes`
- `tickets` — после `bookings`
- `segments` — после `flights` и `tickets`
- `boarding_passes` — после `segments`

---

## Ключевые SQL-паттерны для обучения

### SCD Type 2 (airports_load.sql)

```sql
-- Шаг 1: Получаем актуальный снапшот из STG (последний batch по load_dttm)
WITH latest_stg AS (
    SELECT airport_code, airport_name, city, country, coordinates, timezone
    FROM stg.airports
    WHERE load_dttm = (SELECT MAX(load_dttm) FROM stg.airports)
)
-- Шаг 2: Закрываем изменившиеся версии
UPDATE ods.airports AS o
SET dw_end_date = CURRENT_DATE - 1,
    is_active   = false
FROM latest_stg AS s
WHERE o.airport_code = s.airport_code
  AND o.is_active = true
  AND (
      o.airport_name <> s.airport_name OR
      o.city         <> s.city         OR
      o.country      <> s.country      OR
      COALESCE(o.coordinates, '') <> COALESCE(s.coordinates, '') OR
      o.timezone     <> s.timezone
  );

-- Шаг 3: Вставляем новые записи и новые версии изменённых
INSERT INTO ods.airports (
    airport_code, airport_name, city, country, coordinates, timezone,
    dw_start_date, dw_end_date, is_active, dw_version,
    load_dttm, batch_id
)
SELECT
    s.airport_code,
    s.airport_name,
    s.city,
    s.country,
    s.coordinates,
    s.timezone,
    CURRENT_DATE,
    NULL,
    true,
    COALESCE(
        (SELECT MAX(dw_version) FROM ods.airports WHERE airport_code = s.airport_code),
        0
    ) + 1,
    now(),
    '{{ run_id }}'::text
FROM latest_stg s
WHERE NOT EXISTS (
    SELECT 1 FROM ods.airports o
    WHERE o.airport_code = s.airport_code AND o.is_active = true
);

ANALYZE ods.airports;
```

### UPSERT (bookings_load.sql)

```sql
-- Шаг 1: Обновляем изменившиеся записи (SCD Type 1 для транзакций)
UPDATE ods.bookings AS o
SET book_date    = s.book_date::TIMESTAMP WITH TIME ZONE,
    total_amount = s.total_amount::NUMERIC(10,2),
    load_dttm    = now(),
    batch_id     = '{{ run_id }}'::text
FROM (
    -- Берём последнюю версию каждой записи из STG
    SELECT DISTINCT ON (book_ref)
        book_ref, book_date, total_amount, src_created_at_ts
    FROM stg.bookings
    ORDER BY book_ref, load_dttm DESC
) AS s
WHERE o.book_ref = s.book_ref
  AND (
      o.book_date    <> s.book_date::TIMESTAMP WITH TIME ZONE OR
      o.total_amount <> s.total_amount::NUMERIC(10,2)
  );

-- Шаг 2: Вставляем новые записи
INSERT INTO ods.bookings (book_ref, book_date, total_amount, src_created_at_ts, load_dttm, batch_id)
SELECT
    s.book_ref,
    s.book_date::TIMESTAMP WITH TIME ZONE,
    s.total_amount::NUMERIC(10,2),
    s.src_created_at_ts,
    now(),
    '{{ run_id }}'::text
FROM (
    SELECT DISTINCT ON (book_ref)
        book_ref, book_date, total_amount, src_created_at_ts
    FROM stg.bookings
    ORDER BY book_ref, load_dttm DESC
) AS s
WHERE NOT EXISTS (
    SELECT 1 FROM ods.bookings o WHERE o.book_ref = s.book_ref
);

ANALYZE ods.bookings;
```

### Нетривиальные кастинги (показываем студентам)

```sql
-- duration: '02:35:00' → INTERVAL
s.duration::INTERVAL

-- scheduled_time: 'HH:MM:SS' → TIME
s.scheduled_time::TIME

-- outbound: 'true'/'false' → BOOLEAN
s.outbound::BOOLEAN

-- flight_id: '12345' → INTEGER
s.flight_id::INTEGER

-- boarding_time: '2017-08-13 09:45+03' → TIMESTAMPTZ
s.boarding_time::TIMESTAMP WITH TIME ZONE
```

---

## DQ-проверки ODS

### Справочники (SCD Type 2)

```sql
-- 1. Нет дублей активных записей по бизнес-ключу
SELECT airport_code, COUNT(*)
FROM ods.airports
WHERE is_active
GROUP BY 1
HAVING COUNT(*) > 1;

-- 2. Все STG-ключи присутствуют в ODS (нет потерянных)
SELECT COUNT(*)
FROM (SELECT DISTINCT airport_code FROM stg.airports) s
WHERE NOT EXISTS (
    SELECT 1 FROM ods.airports o
    WHERE o.airport_code = s.airport_code AND o.is_active = true
);

-- 3. Даты корректны: dw_end_date IS NULL для активных
SELECT COUNT(*) FROM ods.airports WHERE is_active AND dw_end_date IS NOT NULL;
```

### Транзакционные (UPSERT)

```sql
-- 1. Нет дублей по бизнес-ключу
SELECT book_ref, COUNT(*) FROM ods.bookings GROUP BY 1 HAVING COUNT(*) > 1;

-- 2. Ссылочная целостность: все tickets ссылаются на существующие bookings
SELECT COUNT(*) FROM ods.tickets t
WHERE NOT EXISTS (SELECT 1 FROM ods.bookings b WHERE b.book_ref = t.book_ref);

-- 3. Все STG-записи попали в ODS
SELECT COUNT(*)
FROM (SELECT DISTINCT book_ref FROM stg.bookings) s
WHERE NOT EXISTS (SELECT 1 FROM ods.bookings o WHERE o.book_ref = s.book_ref);
```

---

## Порядок реализации

1. **`sql/ods/*_ddl.sql`** (9 файлов) — DDL всех таблиц
2. **`sql/ddl_gp_ods.sql`** — мастер-DDL (собирает все `_ddl.sql` через `\i`)
3. **`Makefile`** — таргет `ddl-gp-ods`
4. **`airflow/dags/bookings_ods_ddl.py`** — DDL DAG
5. **`sql/ods/*_load.sql`** (9 файлов) — скрипты загрузки (сначала справочники, потом транзакционные)
6. **`sql/ods/*_dq.sql`** (9 файлов) — DQ-проверки
7. **`airflow/dags/bookings_to_gp_ods.py`** — Load DAG с параллельным графом
8. **`tests/test_dags_smoke.py`** — smoke-тесты для новых DAG
9. **`docs/bookings_to_gp_ods.md`** — документация для студентов

---

## Проверка результата

```bash
make test                  # smoke-тесты: оба новых DAG парсятся без ошибок
make up                    # поднять стек
make ddl-gp-ods            # применить ODS DDL
# Trigger bookings_to_gp_stage → дождаться завершения
# Trigger bookings_to_gp_ods  → проверить параллельный граф в UI
make gp-psql               # проверочные запросы к ods.*
```

Проверочные запросы:

```sql
-- Активные аэропорты = уникальным из STG
SELECT COUNT(*) FROM ods.airports WHERE is_active;
SELECT COUNT(DISTINCT airport_code) FROM stg.airports;

-- Бронирования без дублей
SELECT COUNT(*) FROM ods.bookings;
SELECT COUNT(DISTINCT book_ref) FROM stg.bookings;

-- При первом запуске все SCD-версии = 1
SELECT DISTINCT dw_version FROM ods.airports ORDER BY 1;
```
