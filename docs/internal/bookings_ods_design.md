# ODS Layer: эталонный учебный план реализации (v3)

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
- `routes` — **составной** ключ `(route_no, validity)`: один `route_no` может иметь несколько версий с разными периодами действия. `flights` ссылается только на `route_no` (без `validity`), поэтому при join в DDS нужно будет выбирать подходящую версию маршрута;
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
route_no           TEXT NOT NULL
validity           TEXT NOT NULL
departure_airport  TEXT NOT NULL
arrival_airport    TEXT NOT NULL
airplane_code      TEXT NOT NULL
days_of_week       TEXT
departure_time     TIME              -- STG: scheduled_time (TEXT → TIME)
duration           INTERVAL          -- STG: duration (TEXT → INTERVAL)
_load_id           TEXT NOT NULL
_load_ts           TIMESTAMP NOT NULL DEFAULT now()
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
DISTRIBUTED BY (ticket_no)
```

> В STG `tickets` распределены по `book_ref` для co-location с `bookings` (append-only, lookup не нужен).
> В ODS нужен UPSERT по бизнес-ключу `ticket_no`, поэтому распределяем по нему — иначе каждый lookup потребует redistribute motion.

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

### 4.3. Маппинг STG → ODS (колонки с изменениями)

Большинство бизнес-колонок переносятся 1:1 с приведением типа (`TEXT → ...`). Ниже — только те, где происходит **переименование** или нетривиальное преобразование:

| Таблица | STG колонка | ODS колонка | ODS тип | Комментарий |
|---|---|---|---|---|
| `airplanes` | `range` | `range_km` | `INTEGER` | Явное указание единиц (naming conv.) |
| `airplanes` | `speed` | `speed_kmh` | `INTEGER` | Явное указание единиц (naming conv.) |
| `routes` | `scheduled_time` | `departure_time` | `TIME` | Уточнение смысла |
| `routes` | `duration` | `duration` | `INTERVAL` | Только cast, без rename |
| `tickets` | `outbound` | `is_outbound` | `BOOLEAN` | Префикс `is_` для boolean (naming conv.) |
| `segments` | `price` | `segment_amount` | `NUMERIC(10,2)` | Уточнение: сумма сегмента, не цена билета |
| `flights` | `flight_id` | `flight_id` | `INTEGER` | Только cast TEXT → INT |
| `segments` | `flight_id` | `flight_id` | `INTEGER` | Только cast TEXT → INT |
| `boarding_passes` | `flight_id` | `flight_id` | `INTEGER` | Только cast TEXT → INT |
| все транзакционные | `src_created_at_ts` | `event_ts` | `TIMESTAMP` | Маппинг legacy → канон |
| все | `batch_id` | `_load_id` | `TEXT` | Маппинг legacy → канон |

Пример каста с переименованием в SQL (в CTE):
```sql
s.range::INTEGER       AS range_km,
s.speed::INTEGER       AS speed_kmh,
s.outbound::BOOLEAN    AS is_outbound,
s.price::NUMERIC(10,2) AS segment_amount
```

---

## 5) Контракт батча для ODS

Чтобы ODS был воспроизводимым, в каждом запуске используем **один фиксированный `stg_batch_id`**.

### 5.1. Зачем фиксировать `stg_batch_id`

На проде ETL-оркестратор всегда явно передаёт downstream-задачам идентификатор батча, который прошёл все проверки. Это гарантирует:
- **воспроизводимость**: повторный запуск обработает тот же снимок данных;
- **изоляцию**: ODS не подхватит «сырой» батч, который ещё не прошёл DQ в STG;
- **отладку**: по `_load_id` в ODS легко найти исходные данные в STG.

### 5.2. Как resolve `stg_batch_id` в DAG

В DAG `bookings_to_gp_ods` первым запускается `PythonOperator`, который определяет `stg_batch_id` и кладёт его в XCom:

```python
import logging
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

GREENPLUM_CONN_ID = "greenplum_conn"

def _resolve_stg_batch_id(**context):
    """Определяем stg_batch_id: из dag_run.conf или последний загруженный в STG."""
    conf = context["dag_run"].conf or {}
    stg_batch_id = conf.get("stg_batch_id")

    if not stg_batch_id:
        # Берём batch_id с самым свежим load_dttm (TIMESTAMP, монотонно растёт).
        # MAX(batch_id) ненадёжен: run_id — строка вида "manual__2024-...",
        # лексикографическая сортировка не гарантирует хронологический порядок.
        hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        result = hook.get_first(
            "SELECT batch_id FROM stg.bookings ORDER BY load_dttm DESC LIMIT 1"
        )
        stg_batch_id = result[0] if result and result[0] else None

    if not stg_batch_id:
        raise ValueError(
            "stg_batch_id не найден: передайте в conf или сначала загрузите STG"
        )

    log.info("Используем stg_batch_id = %s", stg_batch_id)
    return stg_batch_id  # автоматически попадёт в XCom как return_value

resolve_batch = PythonOperator(
    task_id="resolve_stg_batch_id",
    python_callable=_resolve_stg_batch_id,
)
```

### 5.3. Как используем в SQL

Во всех `sql/ods/*_load.sql` и `sql/ods/*_dq.sql` значение `stg_batch_id` подставляется через Jinja-шаблон:

```sql
WHERE batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
```

Для читаемости в DAG можно вынести шаблон в константу:

```python
STG_BATCH_ID = "{{ ti.xcom_pull(task_ids='resolve_stg_batch_id') }}"
```

В ODS записываем:
- `_load_id = <stg_batch_id>` — чтобы связать ODS-запись с STG-батчом;
- `_load_ts = now()` — фактическое время загрузки в ODS.

Это простой и понятный паттерн: **один запуск ODS = один снимок STG-батча**.

---

## 6) SQL-паттерны загрузки (SCD1 / UPSERT)

> **Стиль SQL:** в ODS-скриптах используем CTE (Common Table Expressions) вместо вложенных подзапросов — CTE нагляднее, проще для чтения и отладки.

### 6.1. Шаблон для справочника (пример `airports_load.sql`)

> **Важно:** CTE действует в рамках одного SQL-statement. UPDATE и INSERT — два отдельных statement, поэтому CTE `src` дублируется в каждом. Это не ошибка, а необходимость синтаксиса SQL.

```sql
-- Statement 1: UPDATE существующих записей (SCD1 — перезапись при изменении)
WITH src AS (
    SELECT
        airport_code,
        airport_name,
        city,
        country,
        coordinates,
        timezone
    FROM stg.airports
    WHERE batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.airports AS o
SET airport_name = s.airport_name,
    city         = s.city,
    country      = s.country,
    coordinates  = s.coordinates,
    timezone     = s.timezone,
    _load_id     = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts     = now()
FROM src AS s
WHERE o.airport_code = s.airport_code
  AND (
      -- IS DISTINCT FROM — NULL-safe аналог <>:
      -- при NULL с одной стороны <> вернёт NULL (не обновит),
      -- а IS DISTINCT FROM вернёт TRUE (обновит корректно).
      o.airport_name IS DISTINCT FROM s.airport_name OR
      o.city         IS DISTINCT FROM s.city OR
      o.country      IS DISTINCT FROM s.country OR
      o.coordinates  IS DISTINCT FROM s.coordinates OR
      o.timezone     IS DISTINCT FROM s.timezone
  );

-- Statement 2: INSERT новых записей
WITH src AS (
    SELECT
        airport_code,
        airport_name,
        city,
        country,
        coordinates,
        timezone
    FROM stg.airports
    WHERE batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.airports (
    airport_code, airport_name, city, country, coordinates, timezone,
    _load_id, _load_ts
)
SELECT
    s.airport_code, s.airport_name, s.city, s.country, s.coordinates, s.timezone,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text, now()
FROM src s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.airports o
    WHERE o.airport_code = s.airport_code
);

-- Обновляем статистику для оптимизатора запросов Greenplum
ANALYZE ods.airports;
```

### 6.2. Шаблон для транзакции (пример `bookings_load.sql`)

В транзакционных таблицах в одном STG-батче может быть несколько записей с одинаковым бизнес-ключом (например, обновления). Дедуплицируем через `ROW_NUMBER()` — стандартный и явный паттерн, часто встречающийся на собеседованиях и в DE-курсах.

```sql
-- Statement 1: UPDATE существующих записей
WITH src AS (
    SELECT
        book_ref,
        book_date::TIMESTAMP WITH TIME ZONE AS book_date,
        total_amount::NUMERIC(10,2)         AS total_amount,
        src_created_at_ts                    AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY book_ref
            ORDER BY src_created_at_ts DESC NULLS LAST, load_dttm DESC
        ) AS rn
    FROM stg.bookings
    WHERE batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.bookings AS o
SET book_date      = s.book_date,
    total_amount   = s.total_amount,
    event_ts       = s.event_ts,
    _load_id       = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts       = now()
FROM src AS s
WHERE s.rn = 1
  AND o.book_ref = s.book_ref
  AND (
      o.book_date    IS DISTINCT FROM s.book_date OR
      o.total_amount IS DISTINCT FROM s.total_amount
  );

-- Statement 2: INSERT новых записей
WITH src AS (
    SELECT
        book_ref,
        book_date::TIMESTAMP WITH TIME ZONE AS book_date,
        total_amount::NUMERIC(10,2)         AS total_amount,
        src_created_at_ts                    AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY book_ref
            ORDER BY src_created_at_ts DESC NULLS LAST, load_dttm DESC
        ) AS rn
    FROM stg.bookings
    WHERE batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.bookings (
    book_ref, book_date, total_amount, event_ts,
    _load_id, _load_ts
)
SELECT
    s.book_ref, s.book_date, s.total_amount, s.event_ts,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text, now()
FROM src s
WHERE s.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM ods.bookings o
      WHERE o.book_ref = s.book_ref
  );

-- Обновляем статистику для оптимизатора запросов Greenplum
ANALYZE ods.bookings;
```

### 6.3. Идемпотентность паттерна

Паттерн UPDATE + INSERT WHERE NOT EXISTS — **натурально идемпотентен**: повторный запуск с тем же `stg_batch_id` не создаст дублей и не потеряет данные. UPDATE обновит только если атрибуты изменились, INSERT вставит только если бизнес-ключа нет. Это одно из преимуществ подхода.

### 6.4. Поведение при пустом батче

- для инкрементальных таблиц (`bookings`, `tickets`, `flights`, `segments`) пустой батч допустим;
- для snapshot-справочников (`airports`, `airplanes`, `routes`, `seats`) пустой батч считаем ошибкой.

---

## 7) DQ-проверки ODS (минимум, но строго)

Каждый DQ-скрипт должен:
- быть привязан к `stg_batch_id` (через XCom, как в load-скриптах);
- делать `RAISE EXCEPTION` при нарушении;
- давать понятную подсказку в тексте ошибки.

### 7.1. Обязательные проверки

1. **Нет дублей** по бизнес-ключу в ODS.

2. **Покрытие батча:** все ключи из STG текущего батча присутствуют в ODS.

3. **Обязательные поля** не `NULL`/не пустые.

4. **Батч не пустой** для snapshot-справочников (`airports`, `airplanes`, `routes`, `seats`): если STG-батч оказался пустым — это ошибка (источник недоступен или PXF не работает).

5. **Ссылочная целостность** в ODS:
- `tickets.book_ref -> bookings.book_ref`
- `flights.route_no -> routes.route_no` (упрощённая проверка: наличие `route_no` в routes, без учёта конкретной версии `validity`. Это допустимо, потому что в источнике flights ссылается на route_no, а не на конкретную версию маршрута. При downstream join (DDS) нужно будет учитывать период `validity`)
- `segments.ticket_no -> tickets.ticket_no`
- `segments.flight_id -> flights.flight_id`
- `boarding_passes (ticket_no, flight_id) -> segments (ticket_no, flight_id)`

### 7.2. Пример проверки покрытия батча

```sql
SELECT COUNT(*)
FROM (
    SELECT DISTINCT book_ref
    FROM stg.bookings
    WHERE batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
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

Каждый `*_ddl.sql` начинается с `CREATE SCHEMA IF NOT EXISTS ods;` (по аналогии с STG DDL).

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

Корневой таск `resolve_stg_batch_id` определяет батч (см. секцию 5.2), после чего две параллельных ветки стартуют одновременно:

```text
resolve_stg_batch_id
  ├-> load_ods_bookings  -> dq_ods_bookings -> load_ods_tickets -> dq_ods_tickets ──────────────────┐
  ├-> load_ods_airports  -> dq_ods_airports  ─┐                                                     │
  ├-> load_ods_airplanes -> dq_ods_airplanes ─┼-> load_ods_routes -> dq_ods_routes                  │
  └->                                        └-> load_ods_seats  -> dq_ods_seats                    │
                                                                                                     │
                                                  dq_ods_routes -> load_ods_flights -> dq_ods_flights │
                                                                                                     │
                                            dq_ods_flights + dq_ods_tickets -> load_ods_segments -> dq_ods_segments
                                                                                                     │
                                                              dq_ods_segments -> load_ods_boarding_passes -> dq_ods_boarding_passes
                                                                                                     │
                                                        [dq_ods_boarding_passes, dq_ods_seats] -> finish_ods_summary
```

Зависимости (по FK):
- `tickets` после `bookings` (FK: `book_ref`);
- `routes` после `airports` и `airplanes` (FK: `departure_airport`, `arrival_airport`, `airplane_code`);
- `seats` после `airplanes` (FK: `airplane_code`);
- `flights` после `routes` (FK: `route_no`);
- `segments` после `flights` и `tickets` (FK: `flight_id`, `ticket_no`);
- `boarding_passes` после `segments` (FK: `ticket_no`, `flight_id`).

> Справочники (`airports`, `airplanes`) и транзакции (`bookings`) не зависят друг от друга в ODS — данные уже в STG. Поэтому они стартуют параллельно после `resolve_stg_batch_id`.

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
6. Нейминг техполей ODS консистентен с учебной статьёй: `_load_id`, `_load_ts`, `event_ts`.

---

## 12) Как проверять вручную

```bash
make up
make ddl-gp          # создать STG-объекты
make ddl-gp-ods      # создать ODS-объекты
# Trigger bookings_to_gp_stage (загрузить STG)
# Получить batch_id: SELECT batch_id FROM stg.bookings ORDER BY load_dttm DESC LIMIT 1;
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
