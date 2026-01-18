# План ETL для загрузки `bookings.tickets` в STG слой

> Архивный документ: это рабочий план, который использовался при разработке.
> Актуальная реализация потока — DAG `bookings_to_gp_stage` и SQL в `sql/stg/`.

**Ветка:** `chore/bookings-etl`
**Цель:** Добавить загрузку таблицы `tickets` в STG слой Greenplum по аналогии с `bookings`

## 1. Выбор таблицы и обоснование

**Выбранная таблица:** `bookings.tickets`

**Почему `tickets`:**
- Аналитическая ценность: билеты нужны для анализа выручки, загрузки рейсов, пассажиропотока
- Связь с существующим потоком: таблица связана с `bookings.bookings` через `book_ref`
- Инкрементальная природа: билеты создаются вместе с бронированием → понятная логика
- Простая структура: без сложных типов данных (хорошо для обучения)

## 2. Структура исходной таблицы

Исходная таблица в `bookings-db` (Postgres):

| Колонка | Тип | Описание |
|---------|-----|----------|
| `ticket_no` | text (PK) | Уникальный номер билета |
| `book_ref` | text (FK) | Ссылка на бронирование (`bookings.book_ref`) |
| `passenger_id` | text | Идентификатор пассажира |
| `passenger_name` | text | Имя пассажира |
| `outbound` | boolean | Направление рейса (прямой/обратный) |

**Особенности:**
- Количество записей: примерно в 1.5 раза больше, чем бронирований
- Один `book_ref` может иметь несколько `ticket_no`
- В исходной таблице нет явной временной колонки → используем дату из связанного бронирования

## 3. Проектирование STG-таблицы в Greenplum

### Внутренняя таблица `stg.tickets`:

```sql
-- sql/stg/tickets_ddl.sql

CREATE TABLE IF NOT EXISTS stg.tickets (
    -- Бизнес-атрибуты (из источника)
    ticket_no      TEXT NOT NULL,      -- номер билета
    book_ref       TEXT NOT NULL,      -- ссылка на бронирование
    passenger_id   TEXT,               -- идентификатор пассажира
    passenger_name TEXT,               -- имя пассажира
    outbound       TEXT,               -- флаг направления (в источнике boolean; в STG храним как TEXT)
    
    -- Технические атрибуты
    src_created_at_ts TIMESTAMP,        -- временная метка источника (для инкремента)
    load_dttm       TIMESTAMP NOT NULL DEFAULT now(), -- время загрузки в Greenplum
    batch_id        TEXT                -- идентификатор батча (из Airflow run_id)
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
DISTRIBUTED BY (book_ref);             -- распределение по ключу связи с bookings
```

**Решения по проектированию:**
- **DISTRIBUTED BY (book_ref):** типовой джойн `tickets → bookings` идёт по `book_ref`, так меньше motion в MPP
- **boolean → TEXT:** в сыром STG храним бизнес-колонки как TEXT (для обучения и минимизации кастов на входе)
- **src_created_at_ts:** временная метка из даты связанного бронирования (см. раздел 7)

## 4. Проектирование внешней таблицы (PXF)

Используем существующий PXF-профиль для Postgres (как в `stg.bookings_ext`):

```sql
-- sql/stg/tickets_ddl.sql (продолжение)

DROP EXTERNAL TABLE IF EXISTS stg.tickets_ext;

CREATE EXTERNAL TABLE stg.tickets_ext (
    ticket_no      TEXT,
    book_ref       TEXT,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT
)
LOCATION ('pxf://bookings.tickets?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```

**Решения:**
- Только бизнес-атрибуты во внешней таблице (без тех.колонок)
- PXF-профиль `JDBC` (как в `stg.bookings_ext`) — более стабильный вариант
- PXF-сервер настроен как `bookings-db` в конфигурации

## 5. DDL-скрипт (создание таблиц)

Полный файл `sql/stg/tickets_ddl.sql`:

```sql
-- DDL для слоя STG по таблице tickets.
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.tickets через PXF.
DROP EXTERNAL TABLE IF EXISTS stg.tickets_ext;

CREATE EXTERNAL TABLE stg.tickets_ext (
    ticket_no      TEXT,
    book_ref       TEXT,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT
)
LOCATION ('pxf://bookings.tickets?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.tickets — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.tickets (
    ticket_no          TEXT NOT NULL,
    book_ref           TEXT NOT NULL,
    passenger_id       TEXT,
    passenger_name     TEXT,
    outbound           TEXT,
    src_created_at_ts  TIMESTAMP,
    load_dttm          TIMESTAMP NOT NULL DEFAULT now(),
    batch_id           TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Распределяем по book_ref, чтобы джойны tickets → bookings по book_ref были без motion.
DISTRIBUTED BY (book_ref);

-- На случай, если таблица уже была создана раньше с другим ключом распределения.
ALTER TABLE IF EXISTS stg.tickets SET DISTRIBUTED BY (book_ref);
```

## 6. LOAD-скрипт (загрузка инкремента)

### Логика инкремента

**Проблема:** В `bookings.tickets` нет явной временной колонки.

**Решение:** Используем дату бронирования из связанной таблицы `bookings.bookings`:
1. Связь через `tickets.book_ref = bookings.book_ref`
2. Временная колонка: `bookings.book_date`
3. Фильтр инкремента: `book_date > max(src_created_at_ts)`

### Скрипт `sql/stg/tickets_load.sql`:

```sql
-- Загрузка инкремента из stg.tickets_ext в stg.tickets
-- Инкремент определяется по дате бронирования (book_date из bookings.bookings)

INSERT INTO stg.tickets (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    outbound,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.ticket_no,
    ext.book_ref,
    ext.passenger_id,
    ext.passenger_name,
    ext.outbound,
    b.book_date::timestamp,               -- временная метка из бронирования
    now(),
    '{{ run_id }}'::text
FROM stg.tickets_ext AS ext
JOIN stg.bookings_ext AS b ON ext.book_ref = b.book_ref
WHERE b.book_date > COALESCE(
    (
        SELECT max(src_created_at_ts)
        FROM stg.tickets
        WHERE batch_id <> '{{ run_id }}'::text
            OR batch_id IS NULL
    ),
    TIMESTAMP '1900-01-01 00:00:00'
)
AND NOT EXISTS (
    -- Защита от дублей: ticket_no в источнике уникален, и в stg его не дублируем.
    SELECT 1
    FROM stg.tickets AS t
    WHERE t.ticket_no = ext.ticket_no
);
```

**Объяснение логики:**
1. Из `stg.tickets_ext` берём все билеты
2. Прямой JOIN с `stg.bookings_ext` по `book_ref` — это даёт `book_date` из бронирования
3. Фильтр по `book_date > max(src_created_at_ts)` — берём только новые билеты
4. `NOT EXISTS` — защита от повторной загрузки того же билета в текущем батче

**Важное примечание:** Используем только внешние таблицы (`stg.tickets_ext` и `stg.bookings_ext`), так как прямой доступ к `bookings.bookings` через PXF невозможен.

## 7. DQ-проверки (качество данных)

Скрипт `sql/stg/tickets_dq.sql`:

```sql
-- Проверки качества данных для tickets

DO $$
DECLARE
    v_batch_id      TEXT      := '{{ run_id }}'::text;
    v_prev_ts       TIMESTAMP;
    v_source_count  BIGINT;
    v_stg_count     BIGINT;
    v_orphan_count  BIGINT;
    v_null_count    BIGINT;
BEGIN
    -- Опорная метка: максимум src_created_at_ts среди предыдущих батчей
    SELECT max(src_created_at_ts)
    INTO v_prev_ts
    FROM stg.tickets
    WHERE batch_id <> v_batch_id
        OR batch_id IS NULL;

    -- Источник: считаем строки в том же окне инкремента, что и загрузка
    SELECT COUNT(*)
    INTO v_source_count
    FROM stg.tickets_ext AS t
    JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
    WHERE b.book_date > COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');

    IF v_source_count = 0 THEN
        RAISE EXCEPTION
            'В источнике tickets_ext нет строк для окна инкремента (book_date > %). Проверьте генерацию данных (таск generate_bookings_day).',
            COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');
    END IF;

    -- STG: считаем строки текущего батча
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.tickets
    WHERE batch_id = v_batch_id;

    IF v_source_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества билетов. Источник: %, STG (batch_id=%): %',
            v_source_count,
            v_batch_id,
            v_stg_count;
    END IF;

    -- Ссылочная целостность: tickets должны иметь соответствующие bookings в STG
    SELECT COUNT(*)
    INTO v_orphan_count
    FROM stg.tickets AS t
    WHERE t.batch_id = v_batch_id
        AND NOT EXISTS (
            SELECT 1
            FROM stg.bookings AS b
            WHERE b.book_ref = t.book_ref
        );

    IF v_orphan_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены tickets без соответствующих bookings (batch_id=%): %',
            v_batch_id,
            v_orphan_count;
    END IF;

    -- Обязательные поля
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.tickets AS t
    WHERE t.batch_id = v_batch_id
        AND (t.ticket_no IS NULL OR t.book_ref IS NULL);

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены tickets с NULL в обязательных полях (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;
END $$;
```

## 8. Интеграция с существующими DAG

### 8.1. Создание DDL через `bookings_stg_ddl.py`

**Почему расширяем существующий DDL DAG:**
- Уже есть инфраструктура для создания `stg.bookings_ext` и `stg.bookings`
- Единый DAG для создания всех STG-объектов
- Минимальные изменения → проще для новичков

**Изменения в `airflow/dags/bookings_stg_ddl.py`:**

Добавить задачу после создания bookings DDL:

```python
# После существующих задач:

apply_stg_tickets_ddl = PostgresOperator(
    task_id="apply_stg_tickets_ddl",
    postgres_conn_id=GREENPLUM_CONN_ID,
    sql="stg/tickets_ddl.sql",
)

# Обновляем связи задач
apply_stg_bookings_ddl >> apply_stg_tickets_ddl
```

### 8.2. Загрузка данных через `bookings_to_gp_stage.py`

**Почему расширяем существующий загрузочный DAG:**
- `generate_bookings_day` уже есть
- Минимальные изменения → проще для новичков
- Единый поток данных (bookings + tickets за один запуск)

### Изменения в `airflow/dags/bookings_to_gp_stage.py`:

Добавить задачи после загрузки `bookings`:

```python
# После существующих задач:

# Загрузка билетов
load_tickets_to_stg = PostgresOperator(
    task_id="load_tickets_to_stg",
    postgres_conn_id=GREENPLUM_CONN_ID,
    sql="stg/tickets_load.sql",
)

# DQ-проверки билетов
check_tickets_dq = PostgresOperator(
    task_id="check_tickets_dq",
    postgres_conn_id=GREENPLUM_CONN_ID,
    sql="stg/tickets_dq.sql",
)

# Обновляем связи задач
check_row_counts >> load_tickets_to_stg >> check_tickets_dq >> finish_summary
```

**Фактические изменения в DAG:**

Обновлён `description` DAG и добавлены две новые задачи:
- `load_tickets_to_stg` — загружает билеты через PXF
- `check_tickets_dq` — проверяет качество данных билетов

Порядок выполнения:
```
generate_bookings_day 
  → load_bookings_to_stg 
    → check_row_counts 
      → load_tickets_to_stg 
        → check_tickets_dq 
          → finish_summary
```

## 9. Тестирование

### План проверки:

**1. Подготовка окружения:**
```bash
make up                    # поднять стенд
make bookings-init         # инициализировать демо-БД
```

**2. Создание таблиц:**
```bash
# Вариант 1: через Airflow UI (предпочтительно)
# Запустите DAG `bookings_stg_ddl` в Airflow UI

# Вариант 2: через make-команду
make ddl-gp

# Вариант 3: напрямую через psql
make gp-psql
```
```sql
-- внутри psql (если выбрали вариант 3):
\i sql/stg/tickets_ddl.sql
\dt stg.*
```

**3. Первый запуск DAG:**
- Запустить DAG `bookings_to_gp_stage` в Airflow UI
- Проверить успешность всех задач

**4. Проверка данных:**
```sql
-- Количество билетов
SELECT COUNT(*) FROM stg.tickets;

-- Проверка батчей
SELECT batch_id, COUNT(*) 
FROM stg.tickets 
GROUP BY batch_id;

-- Выборка данных
SELECT * FROM stg.tickets 
ORDER BY load_dttm DESC 
LIMIT 10;
```

**5. Инкрементальная загрузка:**
```bash
make bookings-generate-day    # сгенерировать новый день
```
- Повторный запуск DAG
- Проверить, что добавились только новые билеты

**6. Проверка связей с bookings:**
```sql
-- Все билеты должны иметь соответствующие бронирования
SELECT COUNT(*)
FROM stg.tickets t
LEFT JOIN stg.bookings b ON t.book_ref = b.book_ref
WHERE b.book_ref IS NULL;
-- Ожидаемое значение: 0
```

## 10. Порядок реализации

1. ✅ Создать файл `sql/stg/tickets_ddl.sql`
2. ✅ Создать файл `sql/stg/tickets_load.sql`
3. ✅ Создать файл `sql/stg/tickets_dq.sql`
4. ✅ Обновить `airflow/dags/bookings_to_gp_stage.py` (добавить задачи tickets)
5. ✅ Обновить `airflow/dags/bookings_stg_ddl.py` (добавить создание tickets DDL)
6. ✅ Обновить `sql/ddl_gp.sql` (подключить tickets_ddl.sql)
7. ✅ Локальное тестирование (раздел 9)
8. ✅ Проверка через Airflow UI
9. ✅ Проверка идемпотентности DDL
10. ✅ Проверка инкрементальной загрузки
11. ✅ Обновить `README.md` (добавить tickets в список STG-таблиц)

## 11. Сопутствующие изменения

### Изменения в коде:

**sql/ddl_gp.sql:**
- ✅ Добавлено подключение `sql/stg/tickets_ddl.sql`

**educational-tasks.md:**
- ✅ Добавлен раздел 2.3 по анализу структуры `tickets` в STG
- ✅ Добавлен раздел 3.3 по анализу данных `bookings + tickets`

### Необходимые изменения в документации:

**README.md:**
- ✅ Добавить `tickets` в список STG-таблиц
- ✅ Обновить описание DAG `bookings_to_gp_stage` (упомянуть загрузку билетов)

## 12. Результаты тестирования

### Первый запуск (полная загрузка):
- **Загружено:** 182436 билетов
- **Бронирования:** 45730
- **Связи:** все билеты имеют соответствующие бронирования (0 orphan tickets)
- **DQ-проверки:** все пройдены успешно

### Второй запуск (инкрементальная загрузка):
- **Загружено:** новые билеты (количество зависит от сгенерированных данных)
- **Всего в таблице:** сумма всех батчей
- **Уникальность:** все ticket_no уникальные (нет дубликатов)
- **DQ-проверки:** все пройдены успешно

### Идемпотентность DDL:
- **Первый запуск DDL:** таблицы созданы, данные не затронуты
- **Второй запуск DDL:** данные не пропали, таблицы существуют (CREATE TABLE IF NOT EXISTS)
