# ODS Layer: эталонный учебный план реализации (v2)

## Контекст

STG-слой уже реализован как учебный эталон:
- данные из `bookings-db` читаются через PXF;
- в STG бизнес-колонки хранятся как `TEXT`;
- загрузка и DQ работают батчами (`batch_id = {{ run_id }}`).

Этот документ фиксирует **простую и каноничную** реализацию ODS для менти.

---

## 1) Что считаем эталоном для ODS

### 1.1. Роль ODS в этом стенде

ODS в учебном проекте — это:
- типизированные и очищенные данные;
- одна актуальная запись на бизнес-ключ;
- удобный слой для последующей сборки DDS/DM.

### 1.2. Что делаем, что не делаем

Делаем в ODS:
- приведение типов (`TEXT -> TIMESTAMPTZ/NUMERIC/INT/BOOLEAN/...`);
- дедупликацию внутри батча;
- `UPSERT` (SCD Type 1): обновляем текущую запись при изменении, вставляем новые.

Не делаем в ODS (в базовом эталоне):
- SCD Type 2 с периодами действия;
- сложную обработку late-arriving/backdated событий;
- отдельный DQ-слой с хранением результатов.

### 1.3. Где хранится история изменений

- История «как приходили данные» уже сохраняется в STG (append + `batch_id`).
- Историзацию измерений (SCD2) показываем позже в DDS (как в учебной статье `dwh-modeling`).

Итог: **ODS = текущий слой (current state), простой и понятный**.

---

## 2) Нейминг служебных полей (консистентно с de-roadmap)

Источник правил: [`docs/internal/naming_conventions.md`](naming_conventions.md).

В ODS используем такие техполя:

- `_load_id TEXT NOT NULL` — идентификатор загрузки (берём `stg_batch_id`);
- `_load_ts TIMESTAMP NOT NULL DEFAULT now()` — время загрузки в ODS;
- `event_ts TIMESTAMP` — время события из источника (если у сущности оно есть).

### 2.1. Маппинг из текущего STG

- `stg.batch_id` -> `ods._load_id`
- `stg.load_dttm` не переносим 1:1; в ODS пишем собственный `ods._load_ts = now()`
- `stg.src_created_at_ts` -> `ods.event_ts` (для транзакционных таблиц)

### 2.2. Почему так

- нейминг совпадает с учебной статьёй (`_load_id`, `_load_ts`);
- студентам проще переносить паттерн между проектами;
- разделяем «когда событие произошло» (`event_ts`) и «когда загрузили в слой» (`_load_ts`).

---

## 3) Гранулярность и бизнес-ключи ODS

| Таблица | Зерно | Бизнес-ключ |
|---|---|---|
| `ods.airports` | 1 строка = аэропорт | `airport_code` |
| `ods.airplanes` | 1 строка = самолёт | `airplane_code` |
| `ods.routes` | 1 строка = версия маршрута | `(route_no, validity)` |
| `ods.seats` | 1 строка = место в самолёте | `(airplane_code, seat_no)` |
| `ods.bookings` | 1 строка = бронирование | `book_ref` |
| `ods.tickets` | 1 строка = билет | `ticket_no` |
| `ods.flights` | 1 строка = рейс | `flight_id` |
| `ods.segments` | 1 строка = сегмент билета | `(ticket_no, flight_id)` |
| `ods.boarding_passes` | 1 строка = посадочный на сегмент | `(ticket_no, flight_id)` |

Критично для эталона:
- `routes` — **составной** ключ `(route_no, validity)`;
- `boarding_passes` — **составной** ключ `(ticket_no, flight_id)`.

---

## 4) Схема ODS-таблиц (v1, без SCD2)

Ниже — учебный минимум колонок. При необходимости можно добавлять бизнес-атрибуты без изменения паттерна загрузки.

### 4.1. Справочники

#### `ods.airports`
```sql
airport_code  TEXT NOT NULL
airport_name  TEXT NOT NULL
city          TEXT NOT NULL
country       TEXT NOT NULL
coordinates   TEXT
timezone      TEXT NOT NULL
_load_id      TEXT NOT NULL
_load_ts      TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (airport_code)
```

#### `ods.airplanes`
```sql
airplane_code TEXT NOT NULL
model         TEXT NOT NULL
range_km      INTEGER
speed_kmh     INTEGER
_load_id      TEXT NOT NULL
_load_ts      TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (airplane_code)
```

#### `ods.routes`
```sql
route_no            TEXT NOT NULL
validity            TEXT NOT NULL
departure_airport   TEXT NOT NULL
arrival_airport     TEXT NOT NULL
airplane_code       TEXT NOT NULL
days_of_week        TEXT
scheduled_departure_time TIME
scheduled_duration  INTERVAL
_load_id            TEXT NOT NULL
_load_ts            TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (route_no)
```

#### `ods.seats`
```sql
airplane_code   TEXT NOT NULL
seat_no         TEXT NOT NULL
fare_conditions TEXT NOT NULL
_load_id        TEXT NOT NULL
_load_ts        TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (airplane_code)
```

### 4.2. Транзакционные

#### `ods.bookings`
```sql
book_ref      TEXT NOT NULL
book_date     TIMESTAMP WITH TIME ZONE NOT NULL
total_amount  NUMERIC(10,2) NOT NULL
event_ts      TIMESTAMP
_load_id      TEXT NOT NULL
_load_ts      TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (book_ref)
```

#### `ods.tickets`
```sql
ticket_no       TEXT NOT NULL
book_ref        TEXT NOT NULL
passenger_id    TEXT NOT NULL
passenger_name  TEXT NOT NULL
is_outbound     BOOLEAN
event_ts        TIMESTAMP
_load_id        TEXT NOT NULL
_load_ts        TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (book_ref)
```

#### `ods.flights`
```sql
flight_id            INTEGER NOT NULL
route_no             TEXT NOT NULL
status               TEXT NOT NULL
scheduled_departure  TIMESTAMP WITH TIME ZONE
scheduled_arrival    TIMESTAMP WITH TIME ZONE
actual_departure     TIMESTAMP WITH TIME ZONE
actual_arrival       TIMESTAMP WITH TIME ZONE
event_ts             TIMESTAMP
_load_id             TEXT NOT NULL
_load_ts             TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (flight_id)
```

#### `ods.segments`
```sql
ticket_no        TEXT NOT NULL
flight_id        INTEGER NOT NULL
fare_conditions  TEXT NOT NULL
segment_amount   NUMERIC(10,2)
event_ts         TIMESTAMP
_load_id         TEXT NOT NULL
_load_ts         TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (ticket_no)
```

#### `ods.boarding_passes`
```sql
ticket_no      TEXT NOT NULL
flight_id      INTEGER NOT NULL
seat_no        TEXT NOT NULL
boarding_no    INTEGER
boarding_time  TIMESTAMP WITH TIME ZONE
event_ts       TIMESTAMP
_load_id       TEXT NOT NULL
_load_ts       TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (ticket_no)
```

Примечание: в учебном варианте не опираемся на физические `PK/FK`-constraint в Greenplum, а проверяем целостность через DQ-скрипты.

---

## 5) Контракт батча для ODS

Чтобы ODS был воспроизводимым, в каждом запуске используем **один фиксированный `stg_batch_id`**.

### 5.1. Источник `stg_batch_id`

В `bookings_to_gp_ods`:
- принимаем `stg_batch_id` из `dag_run.conf`;
- если не передан — берём последний из `stg.bookings`;
- логируем, какой `stg_batch_id` выбран.

### 5.2. Как применяем

Во всех `sql/ods/*_load.sql`:
- читаем STG только с `WHERE batch_id = :stg_batch_id`;
- пишем в ODS `_load_id = :stg_batch_id`, `_load_ts = now()`.

Это простой и понятный паттерн: **один запуск ODS = один снимок STG-батча**.

---

## 6) SQL-паттерны загрузки (SCD1 / UPSERT)

### 6.1. Шаблон для справочника (пример `airports_load.sql`)

```sql
WITH src AS (
    SELECT
        airport_code,
        airport_name,
        city,
        country,
        coordinates,
        timezone
    FROM stg.airports
    WHERE batch_id = '{{ params.stg_batch_id }}'::text
)
UPDATE ods.airports AS o
SET airport_name = s.airport_name,
    city         = s.city,
    country      = s.country,
    coordinates  = s.coordinates,
    timezone     = s.timezone,
    _load_id     = '{{ params.stg_batch_id }}'::text,
    _load_ts     = now()
FROM src AS s
WHERE o.airport_code = s.airport_code
  AND (
      o.airport_name <> s.airport_name OR
      o.city         <> s.city OR
      o.country      <> s.country OR
      COALESCE(o.coordinates, '') <> COALESCE(s.coordinates, '') OR
      o.timezone     <> s.timezone
  );

INSERT INTO ods.airports (
    airport_code, airport_name, city, country, coordinates, timezone,
    _load_id, _load_ts
)
SELECT
    s.airport_code, s.airport_name, s.city, s.country, s.coordinates, s.timezone,
    '{{ params.stg_batch_id }}'::text, now()
FROM src s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.airports o
    WHERE o.airport_code = s.airport_code
);

ANALYZE ods.airports;
```

### 6.2. Шаблон для транзакции (пример `bookings_load.sql`)

```sql
WITH src AS (
    SELECT DISTINCT ON (book_ref)
        book_ref,
        book_date::TIMESTAMP WITH TIME ZONE AS book_date,
        total_amount::NUMERIC(10,2)         AS total_amount,
        src_created_at_ts                    AS event_ts
    FROM stg.bookings
    WHERE batch_id = '{{ params.stg_batch_id }}'::text
    ORDER BY book_ref, src_created_at_ts DESC NULLS LAST, load_dttm DESC
)
UPDATE ods.bookings AS o
SET book_date      = s.book_date,
    total_amount   = s.total_amount,
    event_ts       = s.event_ts,
    _load_id       = '{{ params.stg_batch_id }}'::text,
    _load_ts       = now()
FROM src AS s
WHERE o.book_ref = s.book_ref
  AND (
      o.book_date    <> s.book_date OR
      o.total_amount <> s.total_amount
  );

INSERT INTO ods.bookings (
    book_ref, book_date, total_amount, event_ts,
    _load_id, _load_ts
)
SELECT
    s.book_ref, s.book_date, s.total_amount, s.event_ts,
    '{{ params.stg_batch_id }}'::text, now()
FROM src s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.bookings o
    WHERE o.book_ref = s.book_ref
);

ANALYZE ods.bookings;
```

### 6.3. Поведение при пустом батче

- для инкрементальных таблиц (`bookings`, `tickets`, `flights`, `segments`) пустой батч допустим;
- для snapshot-справочников (`airports`, `airplanes`, `routes`, `seats`) пустой батч считаем ошибкой.

---

## 7) DQ-проверки ODS (минимум, но строго)

Каждый DQ-скрипт должен:
- быть привязан к `stg_batch_id`;
- делать `RAISE EXCEPTION` при нарушении;
- давать понятную подсказку в тексте ошибки.

### 7.1. Обязательные проверки

1. Нет дублей по бизнес-ключу в ODS.

2. Все ключи из STG текущего батча присутствуют в ODS.

3. Обязательные поля не `NULL`/не пустые.

4. Ссылочная целостность в ODS:
- `tickets.book_ref -> bookings.book_ref`
- `flights.route_no -> routes.route_no`
- `segments.ticket_no -> tickets.ticket_no`
- `segments.flight_id -> flights.flight_id`
- `boarding_passes (ticket_no, flight_id) -> segments (ticket_no, flight_id)`

### 7.2. Пример проверки покрытия батча

```sql
SELECT COUNT(*)
FROM (
    SELECT DISTINCT book_ref
    FROM stg.bookings
    WHERE batch_id = '{{ params.stg_batch_id }}'::text
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.bookings o
    WHERE o.book_ref = s.book_ref
);
```

Ожидаемый результат: `0`.

---

## 8) Структура файлов

```text
sql/ods/
├── airports_ddl.sql
├── airports_load.sql
├── airports_dq.sql
├── airplanes_ddl.sql
├── airplanes_load.sql
├── airplanes_dq.sql
├── routes_ddl.sql
├── routes_load.sql
├── routes_dq.sql
├── seats_ddl.sql
├── seats_load.sql
├── seats_dq.sql
├── bookings_ddl.sql
├── bookings_load.sql
├── bookings_dq.sql
├── tickets_ddl.sql
├── tickets_load.sql
├── tickets_dq.sql
├── flights_ddl.sql
├── flights_load.sql
├── flights_dq.sql
├── segments_ddl.sql
├── segments_load.sql
├── segments_dq.sql
├── boarding_passes_ddl.sql
├── boarding_passes_load.sql
└── boarding_passes_dq.sql

sql/ddl_gp_ods.sql

airflow/dags/
├── bookings_ods_ddl.py
└── bookings_to_gp_ods.py

docs/bookings_to_gp_ods.md
Makefile                (+ ddl-gp-ods)
tests/test_dags_smoke.py (+ smoke для 2 новых DAG)
```

---

## 9) DAG `bookings_to_gp_ods`: учебный граф зависимостей

Принцип: у каждой сущности строго `load -> dq`, и только после `dq` разрешаем downstream.

```text
load_ods_bookings  -> dq_ods_bookings -> load_ods_tickets -> dq_ods_tickets

                    ├-> load_ods_airports  -> dq_ods_airports  ─┐
                    ├-> load_ods_airplanes -> dq_ods_airplanes ─┼-> load_ods_routes -> dq_ods_routes -> load_ods_flights -> dq_ods_flights
                    └->                                        └-> load_ods_seats  -> dq_ods_seats

dq_ods_flights + dq_ods_tickets -> load_ods_segments -> dq_ods_segments -> load_ods_boarding_passes -> dq_ods_boarding_passes

[dq_ods_boarding_passes, dq_ods_seats] -> finish_ods_summary
```

Зависимости:
- `tickets` после `bookings`;
- `routes` после `airports` и `airplanes`;
- `seats` после `airplanes`;
- `flights` после `routes`;
- `segments` после `flights` и `tickets`;
- `boarding_passes` после `segments`.

---

## 10) Порядок реализации

1. Подготовить DDL в `sql/ods/*_ddl.sql`.
2. Сделать мастер-скрипт `sql/ddl_gp_ods.sql`.
3. Добавить `Makefile`-таргет `ddl-gp-ods`.
4. Создать DAG `bookings_ods_ddl.py`.
5. Реализовать `sql/ods/*_load.sql` (SCD1 UPSERT).
6. Реализовать `sql/ods/*_dq.sql`.
7. Создать DAG `bookings_to_gp_ods.py` (с параметром `stg_batch_id`).
8. Дописать smoke-тесты DAG в `tests/test_dags_smoke.py`.
9. Описать запуск и проверки в `docs/bookings_to_gp_ods.md`.

---

## 11) Критерии готовности (Definition of Done)

Готово, если:

1. Оба новых DAG парсятся и проходят smoke-тесты (`make test`).
2. `make ddl-gp-ods` создаёт объекты без ошибок.
3. Для тестового `stg_batch_id` ODS-загрузка завершается успешно.
4. Все DQ-задачи зелёные и реально валят DAG при искусственной ошибке.
5. В ODS нет дублей по бизнес-ключам.
6. Нейминг техполей консистентен с учебной статьёй: `_load_id`, `_load_ts`, `valid_from/valid_to` (последние — когда перейдём к SCD2 в DDS).

---

## 12) Как проверять вручную

```bash
make up
make ddl-gp
# Trigger bookings_to_gp_stage
# Получить batch_id из stg.bookings (последний)
# Trigger bookings_to_gp_ods с conf: {"stg_batch_id": "<значение>"}
make gp-psql
```

Проверочные SQL:

```sql
-- 1) Дубликаты в ODS (пример bookings)
SELECT book_ref, COUNT(*)
FROM ods.bookings
GROUP BY 1
HAVING COUNT(*) > 1;

-- 2) Покрытие текущего STG-батча в ODS
SELECT COUNT(*)
FROM (
    SELECT DISTINCT book_ref
    FROM stg.bookings
    WHERE batch_id = '<stg_batch_id>'
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.bookings o
    WHERE o.book_ref = s.book_ref
);

-- 3) Ссылочная целостность tickets -> bookings
SELECT COUNT(*)
FROM ods.tickets t
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.bookings b
    WHERE b.book_ref = t.book_ref
);
```

Ожидаемо: все три запроса возвращают `0` проблемных строк.

---

## 13) Что будет следующим шагом

После стабилизации ODS:
- строим DDS;
- показываем SCD2 на измерениях DDS (`valid_from`/`valid_to`, `created_at`/`updated_at`) по тому же неймингу, который уже знаком студентам из `dwh-modeling`.
