# План: Валидационный DAG `bookings_validate`

## Контекст и мотивация

### Проблема

Студент реализует 9 объектов (2 ODS, 3 DDS-измерения, 4 DM-витрины) и **не имеет
автоматической обратной связи** — правильно ли работает его код. Сейчас единственный
способ проверки — ручные SQL-запросы и визуальный контроль данных в таблицах.

Это приводит к типичным ошибкам, которые студент не замечает:

| Ошибка | Где проявляется | Когда обнаруживается |
|--------|-----------------|----------------------|
| NULL в PK | ODS/DDS | Только при построении витрины (неожиданные NULL в агрегатах) |
| Дубли по BK | DDS dim_passengers | Факт раздувается, витрины считают неверно |
| SCD2 не закрывает версии | DDS dim_routes | `valid_to` всегда NULL → point-in-time JOIN перестаёт работать |
| «Дыры» в SCD2-интервалах | DDS dim_routes | `sales_report` теряет строки за «пустые» даты |
| TRUNCATE+INSERT не вытащил все строки | ODS airplanes/seats | DDS-измерение неполное → NULL в факте |
| Витрина пуста при непустом источнике | DM | Видно сразу, но причина непонятна |

Без валидационного DAG студент узнаёт о проблеме **через 2-3 слоя** — на этапе DM-витрин,
где отладка многократно сложнее.

### Решение

**Отдельный DAG `bookings_validate`** — набор проверок, сгруппированных по слоям.
Студент запускает его вручную (trigger в Airflow UI) после реализации заданий.
Каждый таск — одна проверка, с дружелюбным сообщением об ошибке и подсказкой,
что делать дальше.

### Педагогическая ценность

1. **Практика чтения логов Airflow** — студент учится находить ошибки в логах тасков
2. **Инкрементальная обратная связь** — можно запускать после каждого шага, не дожидаясь
   реализации всех заданий
3. **Паттерн DQ в пайплайне** — студент видит, как устроены production-проверки качества данных
4. **Активный тест SCD2** — единственный способ убедиться, что SCD2-логика действительно
   работает (справочник `routes` в демо-базе статичен)

### Дизайн-решения

| Решение | Обоснование |
|---------|-------------|
| Отдельный DAG (не часть основного пайплайна) | Запускается по желанию, не блокирует основную загрузку |
| `schedule=None` (ручной триггер) | Студент запускает, когда готов проверить свой код |
| `PostgresOperator` + SQL-скрипты | Единый паттерн с основными DAG-ами |
| SQL в `sql/validate/` | Отдельный каталог — валидация не смешивается с DQ пайплайна |
| `DO $$ ... RAISE EXCEPTION ... $$` | Тот же паттерн, что в эталонных DQ-скриптах |
| TaskGroup по слоям | Студент видит, на каком слое проблема |
| Проверки «существует + не пуста» для DM | Минимально необходимо; бизнес-инварианты студент добавит сам в DQ витрин |

---

## Структура DAG

```
bookings_validate
├── validate_ods (TaskGroup)
│   ├── check_ods_airplanes_rowcount
│   ├── check_ods_seats_rowcount
│   ├── check_ods_no_dup_bk
│   └── check_ods_no_null_pks
├── validate_dds (TaskGroup)
│   ├── check_dim_airplanes_exists
│   ├── check_dim_passengers_exists
│   ├── check_dim_passengers_no_dup_bk
│   ├── check_dim_routes_exists
│   ├── check_dim_routes_scd2          ← активный тест!
│   └── check_dim_routes_no_gaps
└── validate_dm (TaskGroup)
    ├── check_airport_traffic_exists
    ├── check_route_performance_exists
    ├── check_monthly_overview_exists
    └── check_passenger_loyalty_exists
```

### Зависимости между группами

Нет жёстких зависимостей. Все три группы запускаются параллельно — студент может
реализовать только ODS и увидеть зелёные чеки для `validate_ods`, пока DDS/DM ещё
красные. Это даёт инкрементальную обратную связь.

Внутри каждой группы таски также параллельны (независимые проверки).

---

## Детализация проверок

### ODS: `check_ods_airplanes_rowcount`

**Файл:** `sql/validate/ods_airplanes_rowcount.sql`

**Логика:** ODS загружает один конкретный snapshot-батч из STG (по `_load_id`).
STG — append-only: содержит историю всех загрузок. Поэтому сравнивать ODS
со всей STG некорректно — нужно проверять точное множество BK для того батча,
который ODS фактически загрузил.

Определяем батч: `_load_id` из `ods.airplanes` (единственный, т.к. TRUNCATE+INSERT).
Затем проверяем, что множество BK в ODS = множество BK в STG для этого батча.

```sql
DO $$
DECLARE
    v_batch_count BIGINT;
    v_batch TEXT;
    v_ods_count BIGINT;
    v_missing_in_ods BIGINT;
    v_extra_in_ods BIGINT;
BEGIN
    -- ODS пуста?
    SELECT COUNT(*) INTO v_ods_count FROM ods.airplanes;
    IF v_ods_count = 0 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes пуста. Реализуйте загрузку: sql/ods/airplanes_load.sql';
    END IF;

    -- Инвариант: ODS после TRUNCATE+INSERT содержит ровно один _load_id
    SELECT COUNT(DISTINCT _load_id) INTO v_batch_count FROM ods.airplanes;
    IF v_batch_count <> 1 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes содержит % разных _load_id (ожидается 1 после TRUNCATE+INSERT). Проверьте, что load начинается с TRUNCATE.', v_batch_count;
    END IF;

    -- Определяем батч, из которого загружена ODS
    SELECT DISTINCT _load_id INTO v_batch FROM ods.airplanes;

    -- BK есть в STG-батче, но нет в ODS (потеряны при загрузке)
    SELECT COUNT(*) INTO v_missing_in_ods
    FROM (
        SELECT DISTINCT airplane_code FROM stg.airplanes WHERE _load_id = v_batch
    ) AS stg_bk
    WHERE NOT EXISTS (
        SELECT 1 FROM ods.airplanes AS o WHERE o.airplane_code = stg_bk.airplane_code
    );

    IF v_missing_in_ods > 0 THEN
        RAISE EXCEPTION 'FAILED: % самолётов из STG-батча (%) отсутствуют в ods.airplanes. Проверьте логику TRUNCATE+INSERT.', v_missing_in_ods, v_batch;
    END IF;

    -- BK есть в ODS, но нет в STG-батче (откуда взялись?)
    SELECT COUNT(*) INTO v_extra_in_ods
    FROM (
        SELECT DISTINCT airplane_code FROM ods.airplanes
    ) AS ods_bk
    WHERE NOT EXISTS (
        SELECT 1 FROM stg.airplanes AS s WHERE s._load_id = v_batch AND s.airplane_code = ods_bk.airplane_code
    );

    IF v_extra_in_ods > 0 THEN
        RAISE EXCEPTION 'FAILED: % самолётов в ods.airplanes отсутствуют в STG-батче (%). Возможно, TRUNCATE не выполнился перед INSERT.', v_extra_in_ods, v_batch;
    END IF;

    RAISE NOTICE 'PASSED: ods.airplanes содержит % самолётов, множество BK = STG-батч %', v_ods_count, v_batch;
END $$;
```

### ODS: `check_ods_seats_rowcount`

**Файл:** `sql/validate/ods_seats_rowcount.sql`

Аналогичная проверка по составному ключу `(airplane_code, seat_no)` —
точное множество BK из STG-батча (по `_load_id` из `ods.seats`).

### ODS: `check_ods_no_dup_bk`

**Файл:** `sql/validate/ods_no_dup_bk.sql`

**Логика:** Проверить, что в ODS нет дублей по бизнес-ключу. Типичная ошибка
студента — INSERT без предшествующего TRUNCATE, или TRUNCATE забыт при повторном
запуске. Дубли по BK в ODS каскадно ломают DDS (лишние строки в измерениях).

```sql
DO $$
DECLARE
    v_dup_airplanes BIGINT;
    v_dup_seats BIGINT;
BEGIN
    -- airplanes: BK = airplane_code
    SELECT COUNT(*) INTO v_dup_airplanes
    FROM (
        SELECT airplane_code FROM ods.airplanes
        GROUP BY airplane_code HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_airplanes > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes содержит % дублирующихся airplane_code. Проверьте, что load начинается с TRUNCATE.', v_dup_airplanes;
    END IF;

    -- seats: BK = (airplane_code, seat_no)
    SELECT COUNT(*) INTO v_dup_seats
    FROM (
        SELECT airplane_code, seat_no FROM ods.seats
        GROUP BY airplane_code, seat_no HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_seats > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.seats содержит % дублирующихся (airplane_code, seat_no). Проверьте, что load начинается с TRUNCATE.', v_dup_seats;
    END IF;

    RAISE NOTICE 'PASSED: Нет дублей по BK в ods.airplanes и ods.seats';
END $$;
```

### ODS: `check_ods_no_null_pks`

**Файл:** `sql/validate/ods_no_null_pks.sql`

**Логика:** Проверить, что PK-поля не содержат NULL в обеих студенческих ODS-таблицах.

```sql
DO $$
DECLARE
    v_null_airplanes BIGINT;
    v_null_seats BIGINT;
BEGIN
    -- airplanes: PK = airplane_code
    SELECT COUNT(*) INTO v_null_airplanes
    FROM ods.airplanes WHERE airplane_code IS NULL;

    IF v_null_airplanes > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes содержит % строк с NULL в airplane_code.', v_null_airplanes;
    END IF;

    -- seats: PK = (airplane_code, seat_no)
    SELECT COUNT(*) INTO v_null_seats
    FROM ods.seats WHERE airplane_code IS NULL OR seat_no IS NULL;

    IF v_null_seats > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.seats содержит % строк с NULL в PK (airplane_code, seat_no).', v_null_seats;
    END IF;

    RAISE NOTICE 'PASSED: PK не содержат NULL в ods.airplanes и ods.seats';
END $$;
```

---

### DDS: `check_dim_airplanes_exists`

**Файл:** `sql/validate/dim_airplanes_exists.sql`

**Логика:**
1. Таблица не пуста
2. Нет дублей по `airplane_bk`
3. Покрытие ODS: все `airplane_code` из `ods.airplanes` есть в `dds.dim_airplanes`

```sql
DO $$
DECLARE
    v_count BIGINT;
    v_dup BIGINT;
    v_missing BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dds.dim_airplanes;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_airplanes пуста. Реализуйте загрузку: sql/dds/dim_airplanes_load.sql';
    END IF;

    SELECT COUNT(*) - COUNT(DISTINCT airplane_bk) INTO v_dup FROM dds.dim_airplanes;
    IF v_dup > 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_airplanes содержит % дублей по airplane_bk. Проверьте SCD1-логику (UPSERT).', v_dup;
    END IF;

    SELECT COUNT(*) INTO v_missing
    FROM (SELECT DISTINCT airplane_code FROM ods.airplanes) AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_airplanes AS d WHERE d.airplane_bk = s.airplane_code
    );
    IF v_missing > 0 THEN
        RAISE EXCEPTION 'FAILED: % самолётов из ods.airplanes отсутствуют в dds.dim_airplanes.', v_missing;
    END IF;

    RAISE NOTICE 'PASSED: dds.dim_airplanes содержит % строк, покрывает все BK из ODS', v_count;
END $$;
```

### DDS: `check_dim_passengers_exists`

**Файл:** `sql/validate/dim_passengers_exists.sql`

Аналогичная структура: не пуста + нет дублей по `passenger_id` + покрытие
`ods.tickets` (все уникальные `passenger_id` из тикетов есть в измерении).

### DDS: `check_dim_passengers_no_dup_bk`

**Файл:** `sql/validate/dim_passengers_no_dup_bk.sql`

Отдельная проверка дублей `passenger_id` — типичная ошибка студентов при SCD1:
INSERT без проверки EXISTS создаёт дубли. Выделена в отдельный таск для ясности
сообщения об ошибке.

**Примечание:** Эту проверку можно объединить с `check_dim_passengers_exists`.
Отдельный таск оправдан, если хотим дать студенту более точную диагностику.
Решение — на усмотрение реализатора.

### DDS: `check_dim_routes_exists`

**Файл:** `sql/validate/dim_routes_exists.sql`

Таблица не пуста + покрытие ODS (`route_no`) + **не более одной текущей версии
на `route_bk`** (инвариант SCD2: `COUNT(*) WHERE valid_to IS NULL` <= 1 для каждого BK).

Типичная ошибка студента — INSERT без проверки `NOT EXISTS ... AND hashdiff = ...`,
что создаёт дубли текущей версии. Активный SCD2-тест проверяет только один маршрут,
эта проверка ловит проблему глобально.

```sql
DO $$
DECLARE
    v_count BIGINT;
    v_missing BIGINT;
    v_multi_current BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dds.dim_routes;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_routes пуста. Реализуйте загрузку: sql/dds/dim_routes_load.sql';
    END IF;

    -- Покрытие ODS
    SELECT COUNT(*) INTO v_missing
    FROM (SELECT DISTINCT route_no FROM ods.routes) AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_routes AS d WHERE d.route_bk = s.route_no
    );
    IF v_missing > 0 THEN
        RAISE EXCEPTION 'FAILED: % маршрутов из ods.routes отсутствуют в dds.dim_routes.', v_missing;
    END IF;

    -- Инвариант SCD2: не более одной текущей версии на route_bk
    SELECT COUNT(*) INTO v_multi_current
    FROM (
        SELECT route_bk FROM dds.dim_routes
        WHERE valid_to IS NULL
        GROUP BY route_bk HAVING COUNT(*) > 1
    ) AS d;

    IF v_multi_current > 0 THEN
        RAISE EXCEPTION E'FAILED: % маршрутов имеют более одной текущей версии (valid_to IS NULL).\n'
            'Подсказка: при INSERT новой версии проверяйте NOT EXISTS ... AND hashdiff = ...\n'
            'чтобы не создавать дубликат, если hashdiff не изменился.', v_multi_current;
    END IF;

    RAISE NOTICE 'PASSED: dds.dim_routes содержит % строк, покрывает ODS, по одной текущей версии на маршрут', v_count;
END $$;
```

### DDS: `check_dim_routes_scd2` (АКТИВНЫЙ ТЕСТ)

**Файл:** `sql/validate/dim_routes_scd2.sql`

Это ключевая проверка плана. Справочник `bookings.routes` в демо-базе **статичен** —
маршруты не меняются между запусками генератора. При обычном прогоне пайплайна
студент **никогда не увидит**, как SCD2 закрывает старую версию и создаёт новую.

**Алгоритм активного теста:**

```
1. Бэкап: CREATE TABLE _validate_bk_ods_routes AS SELECT * FROM ods.routes;
          CREATE TABLE _validate_bk_dim_routes AS SELECT * FROM dds.dim_routes;
          (обычные таблицы — TEMP не сохраняются между тасками Airflow)

2. Мутация: Выбрать один маршрут из ods.routes (WHERE departure_time IS NOT NULL LIMIT 1).
            Сохранить его route_no в служебную таблицу _validate_scd2_target.
            Обновить в ods.routes его departure_time на +1 час.

3. Запуск студенческого кода: PostgresOperator(sql="dds/dim_routes_load.sql")

4. Проверки (все 5):
   a) Ровно 2 версии тестового маршрута (было 1, стало 2)
   b) Старая версия закрыта: valid_to IS NOT NULL
   c) Ровно 1 открытая версия: valid_to IS NULL
   d) hashdiff старой ≠ hashdiff новой (мутация отразилась в хеше)
   e) Нет «дыры»: старая.valid_to = новая.valid_from

5. Откат (с проверкой существования бэкапов через to_regclass):
   IF _validate_bk_ods_routes exists: TRUNCATE ods.routes + INSERT FROM backup;
   IF _validate_bk_dim_routes exists: TRUNCATE dds.dim_routes + INSERT FROM backup;
   DROP TABLE IF EXISTS _validate_bk_*, _validate_scd2_target;
```

**Реализация:** Этот таск **не может быть простым PostgresOperator** с одним SQL,
потому что шаг 3 — это вызов студенческого SQL-скрипта *внутри* теста. Варианты:

**Выбранный подход: цепочка тасков.**
- Jinja-шаблоны работают из коробки
- Каждый шаг прозрачен в Airflow UI
- `trigger_rule="all_done"` на restore гарантирует откат
- Студент видит в UI, на каком именно шаге проблема

### DDS: `check_dim_routes_no_gaps`

**Файл:** `sql/validate/dim_routes_no_gaps.sql`

**Логика:** Для каждого `route_bk` с несколькими версиями проверить, что
`valid_to` предыдущей версии = `valid_from` следующей (полуоткрытый интервал
`[valid_from, valid_to)` без «дыр»).

**Важно:** SCD2-загрузка поддерживает «исчезнувшие» маршруты (`dim_routes_load.sql`,
Statement 1.1): если маршрут пропал из ODS, его текущая версия закрывается
(`valid_to = CURRENT_DATE`), но новая **не вставляется**. Это корректное поведение —
у такого маршрута `valid_to IS NOT NULL` и `next_valid_from IS NULL`. Проверка
должна считать «дырой» только случаи, когда следующая версия **существует**,
но `valid_to ≠ next_valid_from`.

```sql
DO $$
DECLARE
    v_gaps BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_gaps
    FROM (
        SELECT route_bk, valid_to,
               LEAD(valid_from) OVER (PARTITION BY route_bk ORDER BY valid_from) AS next_valid_from
        FROM dds.dim_routes
    ) AS t
    WHERE valid_to IS NOT NULL
      AND next_valid_from IS NOT NULL   -- следующая версия существует (не «исчезнувший» маршрут)
      AND valid_to <> next_valid_from;

    IF v_gaps > 0 THEN
        RAISE EXCEPTION 'FAILED: В dds.dim_routes найдено % «дыр» между версиями SCD2. valid_to старой версии должен совпадать с valid_from новой.', v_gaps;
    END IF;

    RAISE NOTICE 'PASSED: Нет «дыр» в SCD2-версиях dim_routes';
END $$;
```

---

### DM: `check_{vitrine}_exists` (4 таска)

**Файлы:**
- `sql/validate/airport_traffic_exists.sql`
- `sql/validate/route_performance_exists.sql`
- `sql/validate/monthly_overview_exists.sql`
- `sql/validate/passenger_loyalty_exists.sql`

**Логика:** Минимальная проверка — таблица не пуста + нет NULL в ключевых полях.

Пример для `airport_traffic`:

```sql
DO $$
DECLARE
    v_count BIGINT;
    v_null_pk BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dm.airport_traffic;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dm.airport_traffic пуста. Реализуйте загрузку: sql/dm/airport_traffic_load.sql';
    END IF;

    SELECT COUNT(*) INTO v_null_pk
    FROM dm.airport_traffic
    WHERE traffic_date IS NULL OR airport_sk IS NULL;

    IF v_null_pk > 0 THEN
        RAISE EXCEPTION 'FAILED: dm.airport_traffic содержит % строк с NULL в ключе (traffic_date, airport_sk).', v_null_pk;
    END IF;

    RAISE NOTICE 'PASSED: dm.airport_traffic содержит % строк', v_count;
END $$;
```

Для `route_performance` ключ — `route_bk`, для `monthly_overview` — `(year_actual, month_actual, airplane_sk)`,
для `passenger_loyalty` — `passenger_sk`.

---

## Файлы для создания

### SQL-скрипты (`sql/validate/`)

| # | Файл | Описание |
|---|------|----------|
| 1 | `ods_airplanes_rowcount.sql` | BK coverage: ODS vs STG-батч |
| 2 | `ods_seats_rowcount.sql` | BK coverage: ODS vs STG-батч |
| 3 | `ods_no_dup_bk.sql` | Нет дублей по BK в ODS (airplanes + seats) |
| 4 | `ods_no_null_pks.sql` | NULL в PK обеих ODS-таблиц |
| 5 | `dim_airplanes_exists.sql` | Не пуста + нет дублей BK + покрытие ODS |
| 6 | `dim_passengers_exists.sql` | Не пуста + нет дублей BK + покрытие ODS |
| 7 | `dim_passengers_no_dup_bk.sql` | Дубли `passenger_id` (отдельная диагностика) |
| 8 | `dim_routes_exists.sql` | Не пуста + покрытие ODS + не более 1 текущей версии на BK |
| 9 | `dim_routes_scd2_backup.sql` | Бэкап ods.routes + dds.dim_routes |
| 10 | `dim_routes_scd2_mutate.sql` | Мутация тестового маршрута + сохранение route_no |
| 11 | `dim_routes_scd2_check.sql` | 5 проверок SCD2 после загрузки |
| 12 | `dim_routes_scd2_restore.sql` | Безопасный откат данных из бэкапа |
| 13 | `dim_routes_no_gaps.sql` | Проверка SCD2-интервалов (без «исчезнувших») |
| 14 | `airport_traffic_exists.sql` | Не пуста + NULL в ключе |
| 15 | `route_performance_exists.sql` | Не пуста + NULL в ключе |
| 16 | `monthly_overview_exists.sql` | Не пуста + NULL в ключе |
| 17 | `passenger_loyalty_exists.sql` | Не пуста + NULL в ключе |

### DAG-файл

| # | Файл | Описание |
|---|------|----------|
| 18 | `airflow/dags/bookings_validate.py` | DAG с TaskGroup по слоям |

### Тест

| # | Файл | Описание |
|---|------|----------|
| 19 | Дополнение `tests/test_dags_smoke.py` | Smoke-тест структуры DAG |

---

## DAG-файл: `bookings_validate.py`

```python
"""
Валидационный DAG: самопроверка студенческих заданий.

Запускается вручную в Airflow UI после реализации заданий.
Таски сгруппированы по слоям — студент видит, где именно проблема.
"""

from datetime import timedelta
from logging import getLogger

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

GREENPLUM_CONN_ID = "greenplum_conn"
log = getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 0,                       # Без ретраев — студент должен увидеть ошибку сразу
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="bookings_validate",
    start_date=pendulum.datetime(2017, 1, 1, tz="UTC"),
    schedule=None,                       # Только ручной запуск
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "validate"],
    description="Валидация студенческих заданий: ODS, DDS, DM",
) as dag:

    with TaskGroup("validate_ods") as validate_ods:
        check_ods_airplanes = PostgresOperator(
            task_id="check_ods_airplanes_rowcount",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_airplanes_rowcount.sql",
        )
        check_ods_seats = PostgresOperator(
            task_id="check_ods_seats_rowcount",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_seats_rowcount.sql",
        )
        check_ods_dup_bk = PostgresOperator(
            task_id="check_ods_no_dup_bk",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_no_dup_bk.sql",
        )
        check_ods_pks = PostgresOperator(
            task_id="check_ods_no_null_pks",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_no_null_pks.sql",
        )

    with TaskGroup("validate_dds") as validate_dds:
        check_dim_airplanes = PostgresOperator(
            task_id="check_dim_airplanes_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_airplanes_exists.sql",
        )
        check_dim_passengers = PostgresOperator(
            task_id="check_dim_passengers_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_passengers_exists.sql",
        )
        check_dim_passengers_dup = PostgresOperator(
            task_id="check_dim_passengers_no_dup_bk",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_passengers_no_dup_bk.sql",
        )
        check_dim_routes = PostgresOperator(
            task_id="check_dim_routes_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_exists.sql",
        )

        # SCD2 активный тест: цепочка backup → mutate → load → check → restore
        scd2_backup = PostgresOperator(
            task_id="scd2_backup",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_backup.sql",
        )
        scd2_mutate = PostgresOperator(
            task_id="scd2_mutate",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_mutate.sql",
        )
        scd2_run_load = PostgresOperator(
            task_id="scd2_run_student_load",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="dds/dim_routes_load.sql",          # студенческий load-скрипт!
        )
        scd2_check = PostgresOperator(
            task_id="scd2_check",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_check.sql",
        )
        scd2_restore = PostgresOperator(
            task_id="scd2_restore",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_restore.sql",
            trigger_rule="all_done",                # Откат ВСЕГДА, даже если check упал
        )

        check_dim_routes_gaps = PostgresOperator(
            task_id="check_dim_routes_no_gaps",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_no_gaps.sql",
        )

        # SCD2 цепочка
        scd2_backup >> scd2_mutate >> scd2_run_load >> scd2_check >> scd2_restore

        # no_gaps запускается после restore (на чистых данных)
        scd2_restore >> check_dim_routes_gaps

    with TaskGroup("validate_dm") as validate_dm:
        check_airport_traffic = PostgresOperator(
            task_id="check_airport_traffic_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/airport_traffic_exists.sql",
        )
        check_route_performance = PostgresOperator(
            task_id="check_route_performance_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/route_performance_exists.sql",
        )
        check_monthly_overview = PostgresOperator(
            task_id="check_monthly_overview_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/monthly_overview_exists.sql",
        )
        check_passenger_loyalty = PostgresOperator(
            task_id="check_passenger_loyalty_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/passenger_loyalty_exists.sql",
        )
```

---

## Активный тест SCD2: детализация SQL

### `dim_routes_scd2_backup.sql`

```sql
-- Бэкап текущего состояния перед тестом SCD2.
-- Используем обычные таблицы (не TEMP) — между тасками Airflow
-- TEMP-таблицы не сохраняются (каждый таск = отдельная транзакция).

DROP TABLE IF EXISTS _validate_bk_ods_routes;
CREATE TABLE _validate_bk_ods_routes AS SELECT * FROM ods.routes;

DROP TABLE IF EXISTS _validate_bk_dim_routes;
CREATE TABLE _validate_bk_dim_routes AS SELECT * FROM dds.dim_routes;

-- Cleanup служебной таблицы от предыдущего запуска (на случай если restore не доехал)
DROP TABLE IF EXISTS _validate_scd2_target;
```

### `dim_routes_scd2_mutate.sql`

```sql
-- Мутация: сдвигаем departure_time у одного маршрута на 1 час.
-- Это должно изменить hashdiff → SCD2 должен закрыть старую версию.
--
-- Сохраняем route_no тестового маршрута в служебную таблицу _validate_scd2_target,
-- чтобы check-скрипт точно знал, какой маршрут проверять (а не угадывал по побочным эффектам).

DO $$
DECLARE
    v_route TEXT;
    v_old_time TIME;
BEGIN
    -- Берём первый маршрут, у которого departure_time заполнен
    SELECT route_no, departure_time
    INTO v_route, v_old_time
    FROM ods.routes
    WHERE departure_time IS NOT NULL
    ORDER BY route_no
    LIMIT 1;

    IF v_route IS NULL THEN
        RAISE EXCEPTION 'FAILED: ods.routes пуста или нет маршрутов с departure_time. Загрузите STG→ODS перед проверкой.';
    END IF;

    -- Запоминаем тестовый маршрут в служебную таблицу
    DROP TABLE IF EXISTS _validate_scd2_target;
    CREATE TABLE _validate_scd2_target AS
    SELECT v_route AS route_no;

    -- Сдвигаем время на 1 час у всех записей этого маршрута
    UPDATE ods.routes
    SET departure_time = departure_time + INTERVAL '1 hour'
    WHERE route_no = v_route;

    RAISE NOTICE 'SCD2 TEST: маршрут % — departure_time сдвинут с % на %',
        v_route, v_old_time, v_old_time + INTERVAL '1 hour';
END $$;
```

### `dim_routes_scd2_check.sql`

```sql
-- Проверяем, что SCD2-логика студента сработала корректно.
-- Читаем route_no тестового маршрута из служебной таблицы _validate_scd2_target
-- (создана на шаге mutate), а не угадываем по побочным эффектам.
DO $$
DECLARE
    v_route TEXT;
    v_version_count BIGINT;
    v_closed_count BIGINT;
    v_open_count BIGINT;
    v_old_hash TEXT;
    v_new_hash TEXT;
    v_gap_count BIGINT;
BEGIN
    -- Читаем тестовый маршрут из служебной таблицы
    SELECT route_no INTO v_route FROM _validate_scd2_target LIMIT 1;

    IF v_route IS NULL THEN
        RAISE EXCEPTION 'FAILED: служебная таблица _validate_scd2_target пуста. Шаг mutate не выполнился?';
    END IF;

    -- Если dim_routes пуста — load не запустился
    IF NOT EXISTS (SELECT 1 FROM dds.dim_routes WHERE route_bk = v_route) THEN
        RAISE EXCEPTION E'FAILED: dds.dim_routes не содержит маршрут % после запуска load.\n'
            'Проверьте sql/dds/dim_routes_load.sql.', v_route;
    END IF;

    -- Проверка a: Ровно 2 версии тестового маршрута (было 1, стало 2 после мутации)
    SELECT COUNT(*) INTO v_version_count
    FROM dds.dim_routes WHERE route_bk = v_route;

    IF v_version_count < 2 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — найдена % версия (ожидается 2: старая закрытая + новая открытая).\n'
            'SCD2 должен был создать новую версию после изменения departure_time.', v_route, v_version_count;
    END IF;

    IF v_version_count > 2 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — найдено % версий (ожидается 2).\n'
            'Возможно, load создаёт лишние дубликаты. Проверьте условие NOT EXISTS при INSERT.', v_route, v_version_count;
    END IF;

    -- Проверка b: Старая версия закрыта (valid_to IS NOT NULL)
    SELECT COUNT(*) INTO v_closed_count
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NOT NULL;

    IF v_closed_count = 0 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — SCD2 не закрыл старую версию (valid_to IS NULL у всех версий).\n'
            'Подсказка: hashdiff изменился (departure_time сдвинут на 1 час),\n'
            'но ваш load-скрипт не обнаружил это изменение.\n'
            'Проверьте:\n'
            '  1. Формулу hashdiff — включает ли она departure_time?\n'
            '  2. Логику сравнения hashdiff (UPDATE ... SET valid_to = CURRENT_DATE WHERE hashdiff <> новый_hashdiff)', v_route;
    END IF;

    -- Проверка c: Новая версия открыта (valid_to IS NULL)
    SELECT COUNT(*) INTO v_open_count
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NULL;

    IF v_open_count <> 1 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — ожидается ровно 1 открытая версия (valid_to IS NULL), найдено %.\n'
            'Подсказка: SCD2 должен вставить новую строку с valid_to = NULL.', v_route, v_open_count;
    END IF;

    -- Проверка d: hashdiff старой ≠ hashdiff новой (мутация действительно отразилась)
    SELECT hashdiff INTO v_old_hash
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NOT NULL
    ORDER BY valid_from DESC LIMIT 1;

    SELECT hashdiff INTO v_new_hash
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NULL;

    IF v_old_hash = v_new_hash THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — hashdiff старой и новой версий совпадают.\n'
            'Мутация сдвинула departure_time на 1 час, но hashdiff не изменился.\n'
            'Проверьте, что departure_time входит в формулу hashdiff.', v_route;
    END IF;

    -- Проверка e: Нет «дыры» между valid_to старой и valid_from новой
    SELECT COUNT(*) INTO v_gap_count
    FROM dds.dim_routes AS old_v
    JOIN dds.dim_routes AS new_v
        ON old_v.route_bk = new_v.route_bk
    WHERE old_v.route_bk = v_route
        AND old_v.valid_to IS NOT NULL
        AND new_v.valid_to IS NULL
        AND old_v.valid_to <> new_v.valid_from;

    IF v_gap_count > 0 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — «дыра» между версиями:\n'
            'valid_to старой ≠ valid_from новой.\n'
            'Подсказка: полуоткрытый интервал [valid_from, valid_to).\n'
            'valid_from новой версии должен = valid_to старой (обычно CURRENT_DATE).', v_route;
    END IF;

    RAISE NOTICE 'PASSED: SCD2 корректен для маршрута %. 2 версии, hashdiff различаются, «дыр» нет.', v_route;
END $$;
```

### `dim_routes_scd2_restore.sql`

```sql
-- Откат данных после теста SCD2.
-- Выполняется ВСЕГДА (trigger_rule="all_done"), даже если check упал.
--
-- Безопасность: если backup-шаг не создал таблицы (сбой на backup),
-- откат НЕ трогает live-данные — просто чистит служебные таблицы.
-- Это гарантирует, что restore никогда не сломает ods.routes / dds.dim_routes.

DO $$
DECLARE
    v_has_ods_backup BOOLEAN;
    v_has_dim_backup BOOLEAN;
BEGIN
    -- Проверяем существование backup-таблиц через to_regclass
    -- (ищет по search_path — совпадает с тем, как CREATE TABLE их создал)
    v_has_ods_backup := to_regclass('_validate_bk_ods_routes') IS NOT NULL;
    v_has_dim_backup := to_regclass('_validate_bk_dim_routes') IS NOT NULL;

    -- Восстанавливаем ods.routes только если бэкап существует
    IF v_has_ods_backup THEN
        TRUNCATE ods.routes;
        INSERT INTO ods.routes SELECT * FROM _validate_bk_ods_routes;
        RAISE NOTICE 'RESTORE: ods.routes восстановлена из бэкапа';
    ELSE
        RAISE NOTICE 'RESTORE: бэкап ods.routes не найден — пропускаем (backup-шаг не завершился?)';
    END IF;

    -- Восстанавливаем dds.dim_routes только если бэкап существует
    IF v_has_dim_backup THEN
        TRUNCATE dds.dim_routes;
        INSERT INTO dds.dim_routes SELECT * FROM _validate_bk_dim_routes;
        RAISE NOTICE 'RESTORE: dds.dim_routes восстановлена из бэкапа';
    ELSE
        RAISE NOTICE 'RESTORE: бэкап dds.dim_routes не найден — пропускаем';
    END IF;
END $$;

-- Cleanup служебных таблиц (безусловно, IF EXISTS)
DROP TABLE IF EXISTS _validate_bk_ods_routes;
DROP TABLE IF EXISTS _validate_bk_dim_routes;
DROP TABLE IF EXISTS _validate_scd2_target;

-- Учебный комментарий: Мы восстанавливаем данные из бэкапа, чтобы тест
-- не оставлял «мусорных» версий в dim_routes. Это стандартный паттерн
-- для интеграционных тестов: setup → act → assert → teardown.
-- IF EXISTS проверки гарантируют, что restore безопасен при любом сценарии сбоя.
```

---

## Smoke-тест DAG (дополнение `test_dags_smoke.py`)

Добавить новый тест-класс:

```python
class TestBookingsValidate:
    """Smoke-тесты DAG bookings_validate."""

    def test_dag_loads(self):
        dag = _load_dag("airflow.dags.bookings_validate")
        assert dag is not None

    def test_expected_tasks(self):
        dag = _load_dag("airflow.dags.bookings_validate")
        expected = {
            # ODS
            "validate_ods.check_ods_airplanes_rowcount",
            "validate_ods.check_ods_seats_rowcount",
            "validate_ods.check_ods_no_dup_bk",
            "validate_ods.check_ods_no_null_pks",
            # DDS
            "validate_dds.check_dim_airplanes_exists",
            "validate_dds.check_dim_passengers_exists",
            "validate_dds.check_dim_passengers_no_dup_bk",
            "validate_dds.check_dim_routes_exists",
            "validate_dds.scd2_backup",
            "validate_dds.scd2_mutate",
            "validate_dds.scd2_run_student_load",
            "validate_dds.scd2_check",
            "validate_dds.scd2_restore",
            "validate_dds.check_dim_routes_no_gaps",
            # DM
            "validate_dm.check_airport_traffic_exists",
            "validate_dm.check_route_performance_exists",
            "validate_dm.check_monthly_overview_exists",
            "validate_dm.check_passenger_loyalty_exists",
        }
        assert expected.issubset(dag.task_dict.keys())

    def test_scd2_chain(self):
        """SCD2 цепочка backup → mutate → load → check → restore."""
        dag = _load_dag("airflow.dags.bookings_validate")
        _assert_direct_edge(dag, "validate_dds.scd2_backup", "validate_dds.scd2_mutate")
        _assert_direct_edge(dag, "validate_dds.scd2_mutate", "validate_dds.scd2_run_student_load")
        _assert_direct_edge(dag, "validate_dds.scd2_run_student_load", "validate_dds.scd2_check")
        _assert_direct_edge(dag, "validate_dds.scd2_check", "validate_dds.scd2_restore")

    def test_no_gaps_after_restore(self):
        """no_gaps должен выполняться после restore (на чистых данных)."""
        dag = _load_dag("airflow.dags.bookings_validate")
        _assert_direct_edge(dag, "validate_dds.scd2_restore", "validate_dds.check_dim_routes_no_gaps")

    def test_restore_trigger_rule(self):
        """restore должен выполняться всегда (all_done), даже если check упал."""
        dag = _load_dag("airflow.dags.bookings_validate")
        restore_task = dag.task_dict["validate_dds.scd2_restore"]
        assert restore_task.trigger_rule == "all_done"
```

---

## Порядок реализации

1. Создать каталог `sql/validate/`
2. Написать SQL-скрипты (17 файлов) — начать с простых (ODS, DM), затем DDS, затем SCD2
3. Написать DAG `bookings_validate.py`
4. Дополнить `tests/test_dags_smoke.py`
5. `make test` — smoke-тесты проходят
6. Ручная проверка на стенде (если поднят):
   - Trigger DAG в Airflow UI
   - Все ODS/DDS/DM зелёные (на solution-ветке)
   - SCD2 активный тест: backup → mutate → load → check (зелёный) → restore
7. Коммит

---

## Верификация

### Автоматическая

```bash
make test    # smoke-тест DAG-структуры
```

### Ручная (на стенде)

1. `make up && make ddl-gp` → запустить STG → ODS → DDS → DM
2. Trigger `bookings_validate` в Airflow UI
3. Проверить: все таски зелёные
4. Проверить SCD2: в логах `scd2_check` видно `PASSED: SCD2 корректен для маршрута ...`
5. Проверить restore: `ods.routes` и `dds.dim_routes` не изменились после теста

### Сценарий «студент ещё не реализовал»

На main-ветке (с DDL-заглушками):
- ODS-таски: FAILED (таблицы пусты) → дружелюбное сообщение
- DDS-таски: FAILED → сообщение «Реализуйте загрузку»
- DM-таски: FAILED → сообщение «Реализуйте загрузку»
- SCD2-тест: `scd2_run_student_load` пройдёт (заглушка `SELECT 1;` — валидный SQL),
  но `scd2_check` упадёт (dim_routes не обновилась, версий < 2)
  → `scd2_restore` всё равно выполнится (`trigger_rule="all_done"`)

---

## Открытые вопросы

1. **`dim_passengers_no_dup_bk` — отдельный таск или объединить с `exists`?**
   Отдельный таск даёт точнее диагностику, но увеличивает число тасков.
   Рекомендация: объединить проверки в один файл `dim_passengers_exists.sql`
   (как сделано для `dim_airplanes_exists`).

2. **Нужна ли проверка `total_seats` в `dim_airplanes`?**
   `total_seats` вычисляется агрегацией из `ods.seats`. Если студент забудет
   этот JOIN — поле будет NULL. Можно добавить `SELECT COUNT(*) WHERE total_seats IS NULL`.
   Рекомендация: добавить в `dim_airplanes_exists.sql`.

3. **Бэкап SCD2: обычные таблицы vs TEMP?**
   Каждый таск Airflow — отдельная транзакция → TEMP-таблицы не сохраняются.
   Используем обычные таблицы с префиксом `_validate_bk_`. Риск: если DAG упадёт
   между backup и restore, таблицы останутся. Cleanup: `scd2_restore` делает
   `DROP TABLE IF EXISTS`, поэтому при следующем запуске проблем не будет.

---

## Ключевые файлы

| Файл | Роль |
|------|------|
| `airflow/dags/bookings_validate.py` | DAG: TaskGroup по слоям |
| `sql/validate/*.sql` | 17 SQL-скриптов проверок |
| `sql/dds/dim_routes_load.sql` | Студенческий load (вызывается из SCD2-теста) |
| `tests/test_dags_smoke.py` | Smoke-тесты DAG-структуры |
| `docs/design/assignment_design.md` | Дизайн (секция 4 — источник требований) |
