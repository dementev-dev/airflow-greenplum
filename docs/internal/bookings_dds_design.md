# DDS Layer: план реализации Star Schema для bookings

## Контекст

STG (9 таблиц, TEXT, append-only) и ODS (9 таблиц, типизированные, SCD1) уже реализованы.
Этот план фиксирует реализацию DDS-слоя: Star Schema с измерениями и таблицей фактов.

Формат плана аналогичен `docs/internal/bookings_ods_design.md` — достаточно детальный,
чтобы реализация была однозначной.

---

## 1) Принятые архитектурные решения

| Решение | Выбор | Обоснование |
|---------|-------|-------------|
| Схема БД | Единая `dds` (`dds.dim_*`, `dds.fact_*`) | Проще для студентов, один CREATE SCHEMA |
| Суррогатные ключи | UPSERT + `MAX(sk) + ROW_NUMBER()` | Стабильные SK, Greenplum не поддерживает SERIAL |
| SCD2 | `dim_routes` с hashdiff | Реальная история в данных, классический SCD2 паттерн |
| Остальные измерения | SCD1 UPSERT | Стабильные SK для инкрементального факта |
| Загрузка факта | Инкрементальный UPSERT по `(ticket_no, flight_id)` | Консистентно с ODS, учебная ценность |
| `_load_id` в DDS | `{{ run_id }}` (Airflow run_id) | Не привязан к stg_batch_id, DDS читает current state ODS |

---

## 2) Что создаём

### Измерения (6 штук)

| Таблица | Бизнес-ключ | SK | Тип | Источник ODS |
|---------|-------------|-----|-----|-------------|
| `dds.dim_calendar` | `date_actual` | `calendar_sk` | Статическая (generate_series) | — |
| `dds.dim_airports` | `airport_code` → `airport_bk` | `airport_sk` | SCD1 UPSERT | `ods.airports` |
| `dds.dim_airplanes` | `airplane_code` → `airplane_bk` | `airplane_sk` | SCD1 UPSERT | `ods.airplanes` + `ods.seats` (total_seats) |
| `dds.dim_tariffs` | `fare_conditions` | `tariff_sk` | SCD1 UPSERT | `ods.segments` (DISTINCT) |
| `dds.dim_passengers` | `passenger_id` → `passenger_bk` | `passenger_sk` | SCD1 UPSERT | `ods.tickets` (дедупликация по passenger_id) |
| `dds.dim_routes` | `route_no` → `route_bk` | `route_sk` | **SCD2** (hashdiff) | `ods.routes` (последняя версия по validity) |

### Факт (1 штука)

| Таблица | Зерно | FK на измерения |
|---------|-------|-----------------|
| `dds.fact_flight_sales` | `(ticket_no, flight_id)` — 1 сегмент билета | `calendar_sk`, `departure_airport_sk`, `arrival_airport_sk`, `airplane_sk`, `tariff_sk`, `passenger_sk`, `route_sk` |

---

## 3) DDL таблиц

### 3.1. dds.dim_calendar
```sql
calendar_sk   INTEGER NOT NULL
date_actual   DATE    NOT NULL
year_actual   INTEGER NOT NULL
month_actual  INTEGER NOT NULL
day_actual    INTEGER NOT NULL
day_of_week   INTEGER NOT NULL       -- 1=Пн .. 7=Вс (ISO)
day_name      TEXT    NOT NULL        -- Monday, Tuesday, ...
is_weekend    BOOLEAN NOT NULL
DISTRIBUTED BY (calendar_sk)
```
Статическая, заполняется один раз (2016-01-01 .. 2030-12-31). Без `_load_id`/`_load_ts`.

### 3.2. dds.dim_airports
```sql
airport_sk    INTEGER   NOT NULL
airport_bk    TEXT      NOT NULL      -- airport_code
airport_name  TEXT      NOT NULL
city          TEXT      NOT NULL
country       TEXT      NOT NULL
timezone      TEXT      NOT NULL
coordinates   TEXT
created_at    TIMESTAMP NOT NULL DEFAULT now()
updated_at    TIMESTAMP NOT NULL DEFAULT now()
_load_id      TEXT      NOT NULL
_load_ts      TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (airport_sk)
```

### 3.3. dds.dim_airplanes
```sql
airplane_sk   INTEGER   NOT NULL
airplane_bk   TEXT      NOT NULL      -- airplane_code
model         TEXT      NOT NULL
range_km      INTEGER
speed_kmh     INTEGER
total_seats   INTEGER                 -- COUNT(*) из ods.seats
created_at    TIMESTAMP NOT NULL DEFAULT now()
updated_at    TIMESTAMP NOT NULL DEFAULT now()
_load_id      TEXT      NOT NULL
_load_ts      TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (airplane_sk)
```

### 3.4. dds.dim_tariffs
```sql
tariff_sk       INTEGER   NOT NULL
fare_conditions TEXT      NOT NULL    -- business key = fare_conditions
created_at      TIMESTAMP NOT NULL DEFAULT now()
updated_at      TIMESTAMP NOT NULL DEFAULT now()
_load_id        TEXT      NOT NULL
_load_ts        TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (tariff_sk)
```

### 3.5. dds.dim_passengers
```sql
passenger_sk    INTEGER   NOT NULL
passenger_bk    TEXT      NOT NULL    -- passenger_id
passenger_name  TEXT      NOT NULL
created_at      TIMESTAMP NOT NULL DEFAULT now()
updated_at      TIMESTAMP NOT NULL DEFAULT now()
_load_id        TEXT      NOT NULL
_load_ts        TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (passenger_sk)
```

### 3.6. dds.dim_routes (SCD2)
```sql
route_sk           INTEGER   NOT NULL
route_bk           TEXT      NOT NULL    -- route_no (бизнес-ключ)
departure_airport  TEXT      NOT NULL
arrival_airport    TEXT      NOT NULL
airplane_code      TEXT      NOT NULL
days_of_week       TEXT
departure_time     TIME
duration           INTERVAL
hashdiff           TEXT      NOT NULL    -- md5 хэш атрибутов для детекта изменений
valid_from         DATE      NOT NULL    -- начало действия версии
valid_to           DATE                  -- конец действия (NULL = текущая)
created_at         TIMESTAMP NOT NULL DEFAULT now()
updated_at         TIMESTAMP NOT NULL DEFAULT now()
_load_id           TEXT      NOT NULL
_load_ts           TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (route_sk)
```

Поле `validity` из ODS не переносится как отдельная колонка — DWH сам управляет
версиями через `hashdiff` + `valid_from`/`valid_to` (классический SCD2).
Из ODS берём последнюю версию по `route_no` (ORDER BY validity DESC) как "текущее состояние".

### 3.7. dds.fact_flight_sales
```sql
-- FK на измерения (суррогатные ключи)
calendar_sk            INTEGER
departure_airport_sk   INTEGER
arrival_airport_sk     INTEGER
airplane_sk            INTEGER
tariff_sk              INTEGER
passenger_sk           INTEGER
route_sk               INTEGER

-- Дегенеративные измерения
book_ref    TEXT    NOT NULL
ticket_no   TEXT    NOT NULL
flight_id   INTEGER NOT NULL
book_date   DATE
seat_no     TEXT

-- Метрики
price       NUMERIC(10,2)
is_boarded  BOOLEAN NOT NULL

-- Служебные
_load_id    TEXT      NOT NULL
_load_ts    TIMESTAMP NOT NULL DEFAULT now()
DISTRIBUTED BY (ticket_no)
```

### 3.8. Политика NULL FK в факте

FK суррогатные ключи разделены на три группы:

| Группа | FK | NULL допустим? | Причина |
|--------|-----|---------------|---------|
| **Обязательные** | `tariff_sk`, `passenger_sk` | Нет | Данные всегда есть в ODS (segments, tickets). NULL = баг загрузки. |
| **Зависят от маршрута** | `route_sk`, `departure_airport_sk`, `arrival_airport_sk`, `airplane_sk` | Нет (в норме) | Маршрут должен быть в ODS. NULL = аномалия данных, DQ предупреждает. |
| **Зависят от расписания** | `calendar_sk` | Допустим (редко) | `scheduled_departure` может быть NULL в ODS. DQ считает и логирует, но не фейлит. |

DQ-проверки явно контролируют каждую группу (см. секцию 6).

---

## 4) Нейминг служебных полей (консистентно с naming_conventions.md)

Источник правил: [`docs/internal/naming_conventions.md`](naming_conventions.md).

В DDS используем:

- `_load_id TEXT NOT NULL` — идентификатор загрузки (`{{ run_id }}` Airflow);
- `_load_ts TIMESTAMP NOT NULL DEFAULT now()` — время загрузки в DDS;
- `created_at TIMESTAMP NOT NULL DEFAULT now()` — когда строка создана в таблице;
- `updated_at TIMESTAMP NOT NULL DEFAULT now()` — когда строка обновлена;
- `valid_from DATE NOT NULL` — начало действия версии SCD2;
- `valid_to DATE` — конец действия SCD2 (`NULL` = текущая версия);
- `hashdiff TEXT NOT NULL` — md5 хэш атрибутов для детекта изменений SCD2;
- `*_bk TEXT` — бизнес-ключ измерения (суффикс `_bk`);
- `*_sk INTEGER` — суррогатный ключ измерения (суффикс `_sk`).

### 4.1. Почему `{{ run_id }}` вместо `stg_batch_id`

DDS читает **текущее состояние ODS** (ODS = SCD1, current state). Привязка к stg_batch_id
не требуется. `_load_id` в DDS = Airflow run_id текущего запуска DDS DAG — для аудита
"когда и каким запуском были загружены данные в DDS".

---

## 5) SQL-паттерны загрузки

> **Стиль SQL:** CTE (Common Table Expressions) — как в ODS.

### 5.1. dim_calendar — статическая, INSERT если пуста

```sql
-- Загрузка DDS dim_calendar: статическое измерение (генерация дат).
-- Заполняем только если таблица пуста (идемпотентно).

INSERT INTO dds.dim_calendar (
    calendar_sk, date_actual, year_actual, month_actual,
    day_actual, day_of_week, day_name, is_weekend
)
SELECT
    ROW_NUMBER() OVER (ORDER BY d.date_actual)::INTEGER AS calendar_sk,
    d.date_actual,
    EXTRACT(YEAR FROM d.date_actual)::INTEGER    AS year_actual,
    EXTRACT(MONTH FROM d.date_actual)::INTEGER   AS month_actual,
    EXTRACT(DAY FROM d.date_actual)::INTEGER     AS day_actual,
    EXTRACT(ISODOW FROM d.date_actual)::INTEGER  AS day_of_week,
    TO_CHAR(d.date_actual, 'FMDay')              AS day_name,
    EXTRACT(ISODOW FROM d.date_actual) IN (6, 7) AS is_weekend
FROM (
    SELECT generate_series('2016-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL)::DATE
        AS date_actual
) AS d
WHERE NOT EXISTS (SELECT 1 FROM dds.dim_calendar LIMIT 1);

ANALYZE dds.dim_calendar;
```

### 5.2. dim_airports, dim_airplanes, dim_tariffs, dim_passengers — SCD1 UPSERT

> Все SCD1-измерения используют один и тот же паттерн: UPDATE существующих + INSERT новых
> с `MAX(sk) + ROW_NUMBER()` для стабильных суррогатных ключей.

Паттерн (на примере airports):
```sql
-- Statement 1: UPDATE существующих записей (если атрибуты изменились)
UPDATE dds.dim_airports AS d
SET airport_name = s.airport_name,
    city         = s.city,
    country      = s.country,
    timezone     = s.timezone,
    coordinates  = s.coordinates,
    updated_at   = now(),
    _load_id     = '{{ run_id }}',
    _load_ts     = now()
FROM ods.airports AS s
WHERE d.airport_bk = s.airport_code
  AND (d.airport_name IS DISTINCT FROM s.airport_name
    OR d.city         IS DISTINCT FROM s.city
    OR d.country      IS DISTINCT FROM s.country
    OR d.timezone     IS DISTINCT FROM s.timezone
    OR d.coordinates  IS DISTINCT FROM s.coordinates);

-- Statement 2: INSERT новых записей (MAX(sk) + ROW_NUMBER())
WITH max_sk AS (
    SELECT COALESCE(MAX(airport_sk), 0) AS v FROM dds.dim_airports
)
INSERT INTO dds.dim_airports (
    airport_sk, airport_bk, airport_name, city, country,
    timezone, coordinates, created_at, updated_at, _load_id, _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.airport_code)::INTEGER,
    s.airport_code, s.airport_name, s.city, s.country,
    s.timezone, s.coordinates,
    now(), now(), '{{ run_id }}', now()
FROM ods.airports AS s
WHERE NOT EXISTS (
    SELECT 1 FROM dds.dim_airports d WHERE d.airport_bk = s.airport_code
);

ANALYZE dds.dim_airports;
```

**dim_airplanes** — аналогично, но с LEFT JOIN на `(SELECT airplane_code, COUNT(*) AS total_seats FROM ods.seats GROUP BY 1)` для обогащения `total_seats`.

**dim_tariffs** — аналогично, но источник: `SELECT DISTINCT fare_conditions FROM ods.segments WHERE fare_conditions IS NOT NULL AND fare_conditions <> ''`.

**dim_passengers** — аналогично, но с дедупликацией: `ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY event_ts DESC NULLS LAST, _load_ts DESC, ticket_no DESC)`, берём `rn = 1`. Третий ключ `ticket_no DESC` — стабильный tie-breaker при одинаковых timestamp.

### 5.3. dim_routes — SCD2 с hashdiff

```sql
-- CTE: текущее состояние маршрутов из ODS (последняя версия по validity).
-- В учебных целях используем классический SCD2 с hashdiff для демонстрации
-- паттерна. Хотя у routes в источнике есть поле validity, мы не опираемся
-- на него для версионирования — DWH сам детектит изменения атрибутов через хэш.

-- Statement 1: Закрыть устаревшие версии (valid_to = текущая дата)
WITH src AS (
    SELECT
        route_no,
        departure_airport,
        arrival_airport,
        airplane_code,
        days_of_week,
        departure_time,
        duration,
        md5(
            COALESCE(departure_airport, '') || '|' ||
            COALESCE(arrival_airport, '')   || '|' ||
            COALESCE(airplane_code, '')     || '|' ||
            COALESCE(days_of_week, '')      || '|' ||
            COALESCE(departure_time::TEXT, '') || '|' ||
            COALESCE(duration::TEXT, '')
        ) AS hashdiff,
        ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
    FROM ods.routes
)
UPDATE dds.dim_routes AS d
SET valid_to   = CURRENT_DATE,
    updated_at = now(),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
FROM src AS s
WHERE s.rn = 1
  AND d.route_bk = s.route_no
  AND d.valid_to IS NULL              -- только текущая версия
  AND d.hashdiff <> s.hashdiff;       -- атрибуты изменились

-- Statement 1.1: Закрыть "исчезнувшие" маршруты
-- (есть в текущем срезе DDS, но отсутствуют в текущем состоянии ODS).
WITH src AS (
    SELECT
        route_no,
        ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
    FROM ods.routes
)
UPDATE dds.dim_routes AS d
SET valid_to   = CURRENT_DATE,
    updated_at = now(),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
WHERE d.valid_to IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM src AS s
      WHERE s.rn = 1
        AND s.route_no = d.route_bk
  );

-- Statement 2: Вставить новые версии (для изменённых и совсем новых route_no)
WITH src AS (
    SELECT
        route_no,
        departure_airport,
        arrival_airport,
        airplane_code,
        days_of_week,
        departure_time,
        duration,
        md5(
            COALESCE(departure_airport, '') || '|' ||
            COALESCE(arrival_airport, '')   || '|' ||
            COALESCE(airplane_code, '')     || '|' ||
            COALESCE(days_of_week, '')      || '|' ||
            COALESCE(departure_time::TEXT, '') || '|' ||
            COALESCE(duration::TEXT, '')
        ) AS hashdiff,
        ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
    FROM ods.routes
),
max_sk AS (
    SELECT COALESCE(MAX(route_sk), 0) AS v FROM dds.dim_routes
)
INSERT INTO dds.dim_routes (
    route_sk, route_bk, departure_airport, arrival_airport, airplane_code,
    days_of_week, departure_time, duration,
    hashdiff, valid_from, valid_to, created_at, updated_at, _load_id, _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.route_no)::INTEGER,
    s.route_no,
    s.departure_airport,
    s.arrival_airport,
    s.airplane_code,
    s.days_of_week,
    s.departure_time,
    s.duration,
    s.hashdiff,
    -- valid_from: для совсем новых route_no — sentinel '1900-01-01'
    -- (чтобы point-in-time lookup покрыл все исторические рейсы);
    -- для обновлённых (уже были в DDS, но hashdiff изменился) — CURRENT_DATE.
    CASE
        WHEN EXISTS (
            SELECT 1 FROM dds.dim_routes d2 WHERE d2.route_bk = s.route_no
        ) THEN CURRENT_DATE
        ELSE '1900-01-01'::DATE
    END AS valid_from,
    NULL,             -- valid_to = NULL (текущая версия)
    now(), now(), '{{ run_id }}', now()
FROM src AS s
WHERE s.rn = 1
  AND NOT EXISTS (
      SELECT 1 FROM dds.dim_routes d
      WHERE d.route_bk = s.route_no
        AND d.valid_to IS NULL
        AND d.hashdiff = s.hashdiff
  );

ANALYZE dds.dim_routes;
```

Примечание: `valid_from`/`valid_to` имеют дневную гранулярность (`DATE`).
Если маршрут меняется несколько раз в один день, допускается закрытая версия с
`valid_from = valid_to` (нулевой интервал), чтобы не терять факт изменения.

### 5.4. fact_flight_sales — инкрементальный UPSERT

```sql
-- Statement 1: UPDATE существующих строк факта.
-- ВАЖНО: обновляем ТОЛЬКО мутабельные поля (is_boarded, seat_no, price).
-- Dimension SK (route_sk, airport_sk, airplane_sk и т.д.) НЕ перезаписываем —
-- они зафиксированы на момент INSERT и отражают историческое состояние.
UPDATE dds.fact_flight_sales AS f
SET seat_no    = bp.seat_no,
    price      = seg.segment_amount,
    is_boarded = (bp.ticket_no IS NOT NULL),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
FROM ods.segments AS seg
LEFT JOIN ods.boarding_passes AS bp
    ON bp.ticket_no = seg.ticket_no AND bp.flight_id = seg.flight_id
WHERE f.ticket_no = seg.ticket_no
  AND f.flight_id = seg.flight_id
  AND (f.is_boarded IS DISTINCT FROM (bp.ticket_no IS NOT NULL)
    OR f.price      IS DISTINCT FROM seg.segment_amount
    OR f.seat_no    IS DISTINCT FROM bp.seat_no);

-- Statement 2: INSERT новых строк факта.
-- Dimension SK фиксируются на момент вставки (point-in-time для SCD2 routes).
WITH fact_src AS (
    SELECT
        seg.ticket_no,
        seg.flight_id,
        cal.calendar_sk,
        dep.airport_sk   AS departure_airport_sk,
        arr.airport_sk   AS arrival_airport_sk,
        ap.airplane_sk,
        tar.tariff_sk,
        pax.passenger_sk,
        rte.route_sk,
        tkt.book_ref,
        bkg.book_date::DATE AS book_date,
        bp.seat_no,
        seg.segment_amount  AS price,
        (bp.ticket_no IS NOT NULL) AS is_boarded
    FROM ods.segments AS seg
    JOIN ods.tickets  AS tkt ON tkt.ticket_no = seg.ticket_no
    JOIN ods.bookings AS bkg ON bkg.book_ref  = tkt.book_ref
    JOIN ods.flights  AS flt ON flt.flight_id = seg.flight_id
    -- SCD2 point-in-time: версия маршрута, актуальная на дату вылета
    LEFT JOIN dds.dim_routes AS rte
        ON rte.route_bk = flt.route_no
        AND flt.scheduled_departure::DATE >= rte.valid_from
        AND (rte.valid_to IS NULL OR flt.scheduled_departure::DATE < rte.valid_to)
    LEFT JOIN dds.dim_calendar   AS cal ON cal.date_actual = flt.scheduled_departure::DATE
    LEFT JOIN dds.dim_airports   AS dep ON dep.airport_bk = rte.departure_airport
    LEFT JOIN dds.dim_airports   AS arr ON arr.airport_bk = rte.arrival_airport
    LEFT JOIN dds.dim_airplanes  AS ap  ON ap.airplane_bk = rte.airplane_code
    LEFT JOIN dds.dim_tariffs    AS tar ON tar.fare_conditions = seg.fare_conditions
    LEFT JOIN dds.dim_passengers AS pax ON pax.passenger_bk = tkt.passenger_id
    LEFT JOIN ods.boarding_passes AS bp
        ON bp.ticket_no = seg.ticket_no AND bp.flight_id = seg.flight_id
)
INSERT INTO dds.fact_flight_sales (
    calendar_sk, departure_airport_sk, arrival_airport_sk, airplane_sk,
    tariff_sk, passenger_sk, route_sk,
    book_ref, ticket_no, flight_id, book_date, seat_no,
    price, is_boarded, _load_id, _load_ts
)
SELECT
    s.calendar_sk, s.departure_airport_sk, s.arrival_airport_sk, s.airplane_sk,
    s.tariff_sk, s.passenger_sk, s.route_sk,
    s.book_ref, s.ticket_no, s.flight_id, s.book_date, s.seat_no,
    s.price, s.is_boarded,
    '{{ run_id }}', now()
FROM fact_src AS s
WHERE NOT EXISTS (
    SELECT 1 FROM dds.fact_flight_sales f
    WHERE f.ticket_no = s.ticket_no AND f.flight_id = s.flight_id
);

ANALYZE dds.fact_flight_sales;
```

### 5.5. Модель историчности факта

Dimension SK фиксируются **при INSERT** и не перезаписываются:
- `route_sk` — версия маршрута на дату `scheduled_departure` (point-in-time SCD2 lookup);
- `departure_airport_sk`, `arrival_airport_sk`, `airplane_sk` — из той же версии маршрута;
- `calendar_sk`, `tariff_sk`, `passenger_sk` — из текущих SCD1-измерений на момент INSERT.

UPDATE факта обновляет только **мутабельные поля**: `is_boarded`, `seat_no`, `price`
(появился посадочный, изменилась цена). Это гарантирует, что аналитика по историческим
периодам использует правильные версии измерений.

### 5.6. Политика backfill/reprocess

- **Повторный запуск** с теми же данными ODS — безопасен (идемпотентно).
- **Повторный запуск после изменения маршрутов в ODS**: dim_routes создаст новую SCD2-версию;
  уже вставленные строки факта сохранят старый `route_sk` (историчность).
  Новые строки факта получат актуальный `route_sk` через point-in-time lookup.
- **Полная пересборка факта**: если нужна — `TRUNCATE dds.fact_flight_sales` и повторный
  запуск DAG. Все SK будут пересчитаны через point-in-time lookup.

### 5.7. Идемпотентность паттернов

- **dim_calendar**: `WHERE NOT EXISTS` — повторный запуск не создаёт дублей.
- **SCD1 измерения**: `UPDATE + INSERT WHERE NOT EXISTS` — натурально идемпотентно (как в ODS).
- **SCD2 dim_routes**: `UPDATE changed` + `UPDATE missing` + `INSERT WHERE NOT EXISTS (bk + valid_to IS NULL + hashdiff =)` — повторный запуск с теми же данными ODS не создаёт дублей и не закрывает версии повторно.
- **fact_flight_sales**: `UPDATE + INSERT WHERE NOT EXISTS` — идемпотентно по зерну.

---

## 6) DQ-проверки

Каждый DQ-скрипт: PL/pgSQL `DO $$` блок, `RAISE EXCEPTION` при нарушении (как в ODS).

### 6.1. Обязательные проверки по типам

**Все измерения (кроме calendar):**
1. Таблица не пуста
2. Нет дублей по `_sk`
3. Нет дублей по `_bk` (для SCD1; для SCD2 — нет дублей по `_bk` WHERE `valid_to IS NULL`)
4. Покрытие ODS: все ключи из ODS присутствуют в DDS
5. Обязательные поля не NULL/пустые

**dim_calendar:**
1. Не менее 1000 строк
2. Нет дублей по `calendar_sk` и `date_actual`
3. Обязательные поля не NULL
4. Покрывает диапазон дат из `ods.flights.scheduled_departure` (для NOT NULL)

**dim_routes (SCD2 специфика):**
1. Не более одной текущей версии на `route_bk` (`WHERE valid_to IS NULL` — уникальность)
2. `hashdiff` не NULL/пустой
3. `valid_from` не NULL
4. Корректность интервалов: `valid_from <= valid_to` для всех закрытых версий (DATE-гранулярность)
5. Нет перекрытий версий: для одного `route_bk` интервалы `[valid_from, valid_to)` не пересекаются
6. Покрытие: все `route_no` из ODS имеют хотя бы одну версию в DDS
7. Текущий срез DDS консистентен с ODS: `route_bk` с `valid_to IS NULL` есть в `ods.routes`

**fact_flight_sales:**
1. Таблица не пуста
2. Нет дублей по зерну `(ticket_no, flight_id)`
3. Количество строк = `COUNT(*)` из `ods.segments`
4. **Обязательные FK**: `passenger_sk IS NULL` = 0, `tariff_sk IS NULL` = 0
5. **FK маршрута**: NULL в любом из `route_sk`, `departure_airport_sk`, `arrival_airport_sk`, `airplane_sk` — допустимо при аномалиях, считаем и логируем (`RAISE NOTICE`); фейлим если > 1% строк
6. **Calendar**: `calendar_sk IS NULL` — допустимо если `scheduled_departure IS NULL` в ODS; считаем и логируем (`RAISE NOTICE`); фейлим если > 1% строк
7. Обязательные поля: `book_ref`, `ticket_no`, `flight_id`, `is_boarded` не NULL

### 6.2. Пример DQ для dim_routes (SCD2)

```sql
DO $$
DECLARE
    v_row_count BIGINT;
    v_dup_sk BIGINT;
    v_dup_current BIGINT;
    v_overlap_count BIGINT;
    v_missing_count BIGINT;
    v_orphan_current BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Таблица не пуста
    SELECT COUNT(*) INTO v_row_count FROM dds.dim_routes;
    IF v_row_count = 0 THEN
        RAISE EXCEPTION 'DQ FAILED: dds.dim_routes пуста.';
    END IF;

    -- Нет дублей по SK
    SELECT COUNT(*) - COUNT(DISTINCT route_sk) INTO v_dup_sk FROM dds.dim_routes;
    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены дубликаты route_sk: %', v_dup_sk;
    END IF;

    -- SCD2: корректность интервалов (valid_from <= valid_to для закрытых версий)
    SELECT COUNT(*) INTO v_null_count
    FROM dds.dim_routes
    WHERE valid_to IS NOT NULL AND valid_from > valid_to;
    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены версии с valid_from > valid_to: %',
            v_null_count;
    END IF;

    -- SCD2: нет перекрытий интервалов для одного route_bk
    SELECT COUNT(*) INTO v_overlap_count
    FROM (
        SELECT 1
        FROM dds.dim_routes d1
        JOIN dds.dim_routes d2
            ON d1.route_bk = d2.route_bk
            AND d1.route_sk < d2.route_sk
            AND d1.valid_from < COALESCE(d2.valid_to, DATE '9999-12-31')
            AND d2.valid_from < COALESCE(d1.valid_to, DATE '9999-12-31')
    ) AS overlaps;
    IF v_overlap_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены перекрытия SCD2-интервалов: %',
            v_overlap_count;
    END IF;

    -- SCD2: не более одной текущей версии на route_bk
    SELECT COUNT(*) INTO v_dup_current
    FROM (
        SELECT route_bk
        FROM dds.dim_routes
        WHERE valid_to IS NULL
        GROUP BY route_bk
        HAVING COUNT(*) > 1
    ) AS d;
    IF v_dup_current <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены route_bk с > 1 текущей версией: %',
            v_dup_current;
    END IF;

    -- Покрытие ODS (все route_no имеют хотя бы одну версию)
    SELECT COUNT(*) INTO v_missing_count
    FROM (SELECT DISTINCT route_no FROM ods.routes) AS o
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_routes d WHERE d.route_bk = o.route_no
    );
    IF v_missing_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes отсутствуют маршруты из ODS: %', v_missing_count;
    END IF;

    -- SCD2: current-срез DDS не содержит route_bk, которых нет в ODS
    SELECT COUNT(*) INTO v_orphan_current
    FROM (
        SELECT DISTINCT route_bk
        FROM dds.dim_routes
        WHERE valid_to IS NULL
    ) AS d
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.routes AS o
        WHERE o.route_no = d.route_bk
    );
    IF v_orphan_current <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в current-срезе dds.dim_routes есть route_bk вне ODS: %',
            v_orphan_current;
    END IF;

    -- Обязательные поля
    SELECT COUNT(*) INTO v_null_count
    FROM dds.dim_routes
    WHERE route_sk IS NULL
        OR route_bk IS NULL OR route_bk = ''
        OR departure_airport IS NULL OR departure_airport = ''
        OR arrival_airport IS NULL OR arrival_airport = ''
        OR airplane_code IS NULL OR airplane_code = ''
        OR hashdiff IS NULL OR hashdiff = ''
        OR valid_from IS NULL
        OR created_at IS NULL
        OR updated_at IS NULL
        OR _load_id IS NULL OR _load_id = ''
        OR _load_ts IS NULL;
    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены NULL обязательные поля: %', v_null_count;
    END IF;

    RAISE NOTICE 'DQ PASSED: dds.dim_routes ок, строк=% (версий)', v_row_count;
END $$;
```

### 6.3. Пример DQ для fact_flight_sales

```sql
DO $$
DECLARE
    v_row_count BIGINT;
    v_ods_count BIGINT;
    v_dup_count BIGINT;
    v_null_passenger BIGINT;
    v_null_tariff BIGINT;
    v_null_route_related BIGINT;
    v_null_calendar BIGINT;
    v_null_required BIGINT;
BEGIN
    -- Таблица не пуста
    SELECT COUNT(*) INTO v_row_count FROM dds.fact_flight_sales;
    IF v_row_count = 0 THEN
        RAISE EXCEPTION 'DQ FAILED: dds.fact_flight_sales пуста.';
    END IF;

    -- Покрытие: количество строк = ods.segments
    SELECT COUNT(*) INTO v_ods_count FROM ods.segments;
    IF v_row_count <> v_ods_count THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.fact_flight_sales (%) <> ods.segments (%). Потеряны строки.',
            v_row_count, v_ods_count;
    END IF;

    -- Нет дублей по зерну
    SELECT COUNT(*) INTO v_dup_count
    FROM (
        SELECT ticket_no, flight_id
        FROM dds.fact_flight_sales
        GROUP BY ticket_no, flight_id
        HAVING COUNT(*) > 1
    ) AS d;
    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.fact_flight_sales дубликаты (ticket_no, flight_id): %',
            v_dup_count;
    END IF;

    -- Ссылочная целостность: passenger_sk
    SELECT COUNT(*) INTO v_null_passenger
    FROM dds.fact_flight_sales WHERE passenger_sk IS NULL;
    IF v_null_passenger <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в fact_flight_sales строки без passenger_sk: %', v_null_passenger;
    END IF;

    -- Ссылочная целостность: tariff_sk
    SELECT COUNT(*) INTO v_null_tariff
    FROM dds.fact_flight_sales WHERE tariff_sk IS NULL;
    IF v_null_tariff <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в fact_flight_sales строки без tariff_sk: %', v_null_tariff;
    END IF;

    -- FK маршрута: route-related группа (допустимо при аномалиях, фейлим если > 1%)
    SELECT COUNT(*) INTO v_null_route_related
    FROM dds.fact_flight_sales
    WHERE route_sk IS NULL
        OR departure_airport_sk IS NULL
        OR arrival_airport_sk IS NULL
        OR airplane_sk IS NULL;
    IF v_null_route_related > 0 THEN
        IF v_null_route_related * 100.0 / NULLIF(v_row_count, 0) > 1.0 THEN
            RAISE EXCEPTION
                'DQ FAILED: в fact_flight_sales слишком много строк с NULL в route-related FK: % (>1%%)',
                v_null_route_related;
        ELSE
            RAISE NOTICE
                'DQ WARNING: в fact_flight_sales строк с NULL в route-related FK: % (<=1%%, допустимо)',
                v_null_route_related;
        END IF;
    END IF;

    -- Calendar: calendar_sk (допустимо если scheduled_departure IS NULL)
    SELECT COUNT(*) INTO v_null_calendar
    FROM dds.fact_flight_sales WHERE calendar_sk IS NULL;
    IF v_null_calendar > 0 THEN
        IF v_null_calendar * 100.0 / NULLIF(v_row_count, 0) > 1.0 THEN
            RAISE EXCEPTION
                'DQ FAILED: в fact_flight_sales слишком много строк без calendar_sk: % (>1%%)',
                v_null_calendar;
        ELSE
            RAISE NOTICE
                'DQ WARNING: в fact_flight_sales строк без calendar_sk: % (<=1%%, допустимо)',
                v_null_calendar;
        END IF;
    END IF;

    -- Обязательные поля
    SELECT COUNT(*) INTO v_null_required
    FROM dds.fact_flight_sales
    WHERE book_ref IS NULL OR book_ref = ''
        OR ticket_no IS NULL OR ticket_no = ''
        OR flight_id IS NULL
        OR is_boarded IS NULL
        OR _load_id IS NULL OR _load_id = ''
        OR _load_ts IS NULL;
    IF v_null_required <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в fact_flight_sales NULL обязательные поля: %', v_null_required;
    END IF;

    RAISE NOTICE 'DQ PASSED: dds.fact_flight_sales ок, строк=%', v_row_count;
END $$;
```

---

## 7) Структура файлов

```text
sql/dds/                               (21 SQL-файл)
├── dim_calendar_ddl.sql
├── dim_calendar_load.sql
├── dim_calendar_dq.sql
├── dim_airports_ddl.sql
├── dim_airports_load.sql
├── dim_airports_dq.sql
├── dim_airplanes_ddl.sql
├── dim_airplanes_load.sql
├── dim_airplanes_dq.sql
├── dim_tariffs_ddl.sql
├── dim_tariffs_load.sql
├── dim_tariffs_dq.sql
├── dim_passengers_ddl.sql
├── dim_passengers_load.sql
├── dim_passengers_dq.sql
├── dim_routes_ddl.sql
├── dim_routes_load.sql
├── dim_routes_dq.sql
├── fact_flight_sales_ddl.sql
├── fact_flight_sales_load.sql
└── fact_flight_sales_dq.sql

airflow/dags/                           (2 новых DAG)
├── bookings_dds_ddl.py
└── bookings_to_gp_dds.py

sql/ddl_gp.sql                         (+ \i dds/*_ddl.sql в конец)
tests/test_dags_smoke.py               (+ 2 smoke-теста)
docs/bookings_to_gp_dds.md             (документация для студентов)
docs/internal/bookings_dds_design.md    (этот план)
docs/internal/db_schema.md             (обновить: добавить dim_routes, статус DDS)
```

---

## 8) DAG `bookings_dds_ddl`

По аналогии с `bookings_ods_ddl.py` (`airflow/dags/bookings_ods_ddl.py`).

**Ключевые параметры:**
- `dag_id = "bookings_dds_ddl"`
- `schedule = None`
- `template_searchpath = "/sql"`
- `tags = ["demo", "greenplum", "ddl", "bookings", "dds"]`
- `description = "Учебный DDL DAG: создаёт/обновляет dds.* для bookings"`

**Задачи (линейная цепочка из 7 задач):**
1. `apply_dds_dim_calendar_ddl` — `dds/dim_calendar_ddl.sql`
2. `apply_dds_dim_airports_ddl` — `dds/dim_airports_ddl.sql`
3. `apply_dds_dim_airplanes_ddl` — `dds/dim_airplanes_ddl.sql`
4. `apply_dds_dim_tariffs_ddl` — `dds/dim_tariffs_ddl.sql`
5. `apply_dds_dim_passengers_ddl` — `dds/dim_passengers_ddl.sql`
6. `apply_dds_dim_routes_ddl` — `dds/dim_routes_ddl.sql`
7. `apply_dds_fact_flight_sales_ddl` — `dds/fact_flight_sales_ddl.sql`

---

## 9) DAG `bookings_to_gp_dds`: граф зависимостей

По аналогии с `bookings_to_gp_ods.py` (`airflow/dags/bookings_to_gp_ods.py`).

**Ключевые параметры:**
- `dag_id = "bookings_to_gp_dds"`
- `schedule = None`, `max_active_runs = 1`
- `_load_id = {{ run_id }}` (не нужен `resolve_stg_batch_id`)

### 9.1. Граф

```text
load_dds_dim_calendar -> dq_dds_dim_calendar
    |
    v (после calendar — параллельно 5 измерений)
load_dds_dim_airports   -> dq_dds_dim_airports
load_dds_dim_airplanes  -> dq_dds_dim_airplanes
load_dds_dim_tariffs    -> dq_dds_dim_tariffs
load_dds_dim_passengers -> dq_dds_dim_passengers
load_dds_dim_routes     -> dq_dds_dim_routes
    |
    v (факт после ВСЕХ 6 измерений)
load_dds_fact_flight_sales -> dq_dds_fact_flight_sales -> finish_dds_summary
```

Задач: 7 load + 7 dq + 1 finish = **15 задач**.

### 9.2. Зависимости (Python)

```python
load_dds_dim_calendar >> dq_dds_dim_calendar

# 5 измерений параллельно после calendar
dq_dds_dim_calendar >> [
    load_dds_dim_airports, load_dds_dim_airplanes,
    load_dds_dim_tariffs, load_dds_dim_passengers,
    load_dds_dim_routes
]

load_dds_dim_airports >> dq_dds_dim_airports
load_dds_dim_airplanes >> dq_dds_dim_airplanes
load_dds_dim_tariffs >> dq_dds_dim_tariffs
load_dds_dim_passengers >> dq_dds_dim_passengers
load_dds_dim_routes >> dq_dds_dim_routes

# Факт после всех измерений
[dq_dds_dim_airports, dq_dds_dim_airplanes,
 dq_dds_dim_tariffs, dq_dds_dim_passengers,
 dq_dds_dim_routes] >> load_dds_fact_flight_sales

load_dds_fact_flight_sales >> dq_dds_fact_flight_sales >> finish_dds_summary
```

### 9.3. Почему calendar первая

Факт ссылается на `calendar_sk`. Calendar — статическая таблица, заполняется один раз.
Но если DDS запускается впервые, calendar должна быть заполнена до загрузки факта.
Остальные 5 измерений не зависят друг от друга в DDS (FK-зависимости уже проверены в ODS).

---

## 10) Smoke-тесты

Добавить в `tests/test_dags_smoke.py` два теста:

### test_bookings_dds_ddl_dag_structure
- 7 задач: `apply_dds_dim_{calendar,airports,airplanes,tariffs,passengers,routes}_ddl`, `apply_dds_fact_flight_sales_ddl`
- Линейная цепочка: каждая задача reachable от предыдущей

### test_bookings_to_gp_dds_dag_structure
- 15 задач (7 load + 7 dq + `finish_dds_summary`)
- `load → dq` для каждого объекта (direct edge)
- `dq_dds_dim_calendar` → все 5 остальных load-измерений
- airports и airplanes не зависят друг от друга (параллельность)
- факт reachable от всех 6 dq измерений (через `[...] >> load_dds_fact`)
- `finish_dds_summary` reachable от `dq_dds_fact_flight_sales`

---

## 11) Порядок реализации

1. DDL: 7 файлов `sql/dds/*_ddl.sql` (calendar, airports, airplanes, tariffs, passengers, routes, fact)
2. Подключить DDL в `sql/ddl_gp.sql` (добавить `\i dds/*_ddl.sql`)
3. DAG `airflow/dags/bookings_dds_ddl.py`
4. Load SQL: 7 файлов `sql/dds/*_load.sql`
5. DQ SQL: 7 файлов `sql/dds/*_dq.sql`
6. DAG `airflow/dags/bookings_to_gp_dds.py`
7. Smoke-тесты в `tests/test_dags_smoke.py` (+2 теста)
8. Документация `docs/bookings_to_gp_dds.md`
9. Обновить `docs/internal/db_schema.md` — отразить `dim_routes` и актуальный статус DDS

Итого: **21 SQL-файл** + **2 DAG** + **обновления 3 существующих файлов** + **1 новый doc-файл**.

---

## 12) Критические файлы-образцы (patterns to follow)

| Что реализуем | Образец в репозитории |
|---------------|---------|
| DDS DDL DAG | `airflow/dags/bookings_ods_ddl.py` |
| DDS ETL DAG | `airflow/dags/bookings_to_gp_ods.py` |
| DDL SQL | `sql/ods/airports_ddl.sql` |
| SCD1 UPSERT SQL | `sql/ods/airports_load.sql`, `sql/ods/bookings_load.sql` |
| DQ SQL (PL/pgSQL) | `sql/ods/airports_dq.sql`, `sql/ods/segments_dq.sql` |
| Smoke-тесты | `tests/test_dags_smoke.py` (тесты ODS DAG) |
| Подключение DDL | `sql/ddl_gp.sql` (секция DDS `\i` директивы) |

---

## 13) Критерии готовности (Definition of Done)

1. Оба новых DAG парсятся и проходят smoke-тесты (`make test`)
2. `make ddl-gp` создаёт STG+ODS+DDS без ошибок
3. DAG `bookings_to_gp_dds` завершается успешно после ODS
4. Все DQ-задачи зелёные
5. В DDS нет дублей по SK; для SCD1 нет дублей по BK, для SCD2 не более одной current-версии BK
6. `fact_flight_sales` содержит столько строк, сколько в `ods.segments`
7. Нейминг консистентен: `_bk`, `_sk`, `valid_from`/`valid_to`, `hashdiff`, `_load_id`, `_load_ts`, `created_at`/`updated_at`
8. `make fmt` / `make lint` проходят
9. `dim_routes` демонстрирует SCD2 с реальными версиями

---

## 14) Как проверять вручную

```bash
make up
make ddl-gp          # создать STG+ODS+DDS-объекты
# Trigger bookings_to_gp_stage (загрузить STG)
# Trigger bookings_to_gp_ods   (загрузить ODS)
# Trigger bookings_to_gp_dds   (загрузить DDS)
make gp-psql
```

Проверочные SQL:

```sql
-- 1) Количество строк в измерениях и факте
SELECT 'dim_calendar'      AS tbl, COUNT(*) FROM dds.dim_calendar
UNION ALL
SELECT 'dim_airports',              COUNT(*) FROM dds.dim_airports
UNION ALL
SELECT 'dim_airplanes',             COUNT(*) FROM dds.dim_airplanes
UNION ALL
SELECT 'dim_tariffs',               COUNT(*) FROM dds.dim_tariffs
UNION ALL
SELECT 'dim_passengers',            COUNT(*) FROM dds.dim_passengers
UNION ALL
SELECT 'dim_routes',                COUNT(*) FROM dds.dim_routes
UNION ALL
SELECT 'fact_flight_sales',         COUNT(*) FROM dds.fact_flight_sales;

-- 2) Покрытие факта: должно совпадать с ods.segments
SELECT
    (SELECT COUNT(*) FROM dds.fact_flight_sales) AS fact_rows,
    (SELECT COUNT(*) FROM ods.segments)          AS ods_rows;

-- 3) SCD2 dim_routes: версии маршрутов
SELECT route_bk, COUNT(*) AS versions
FROM dds.dim_routes
GROUP BY route_bk
HAVING COUNT(*) > 1
ORDER BY versions DESC;

-- 4) NULL суррогатные ключи в факте (потенциальные аномалии)
SELECT
    SUM(CASE WHEN calendar_sk IS NULL THEN 1 ELSE 0 END)           AS null_calendar,
    SUM(CASE WHEN departure_airport_sk IS NULL THEN 1 ELSE 0 END)  AS null_dep_airport,
    SUM(CASE WHEN arrival_airport_sk IS NULL THEN 1 ELSE 0 END)    AS null_arr_airport,
    SUM(CASE WHEN airplane_sk IS NULL THEN 1 ELSE 0 END)           AS null_airplane,
    SUM(CASE WHEN tariff_sk IS NULL THEN 1 ELSE 0 END)             AS null_tariff,
    SUM(CASE WHEN passenger_sk IS NULL THEN 1 ELSE 0 END)          AS null_passenger,
    SUM(CASE WHEN route_sk IS NULL THEN 1 ELSE 0 END)              AS null_route
FROM dds.fact_flight_sales;

-- 5) Пример аналитического запроса: выручка по тарифам
SELECT
    t.fare_conditions,
    COUNT(*)        AS segments,
    SUM(f.price)    AS total_revenue,
    AVG(f.price)    AS avg_price
FROM dds.fact_flight_sales AS f
JOIN dds.dim_tariffs AS t ON t.tariff_sk = f.tariff_sk
GROUP BY t.fare_conditions
ORDER BY total_revenue DESC;

-- 6) Пример запроса с SCD2: маршруты и их версии
SELECT
    r.route_bk,
    r.departure_airport,
    r.arrival_airport,
    r.airplane_code,
    r.valid_from,
    r.valid_to,
    COUNT(f.ticket_no) AS fact_rows
FROM dds.dim_routes AS r
LEFT JOIN dds.fact_flight_sales AS f ON f.route_sk = r.route_sk
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY r.route_bk, r.valid_from;
```

---

## 15) Что будет следующим шагом

- Data Mart (витрина) поверх DDS
- Unknown-member стратегия (`*_sk = 0`) для late-arriving dimensions
- `dim_calendar.is_holiday` (если появится источник)
