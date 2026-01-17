# План ETL для загрузки `bookings.tickets` в STG слой

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

DROP TABLE IF EXISTS stg.tickets CASCADE;

CREATE TABLE stg.tickets (
    -- Бизнес-атрибуты (из источника)
    ticket_no      TEXT NOT NULL,      -- номер билета
    book_ref       TEXT NOT NULL,      -- ссылка на бронирование
    passenger_id   TEXT,               -- идентификатор пассажира
    passenger_name TEXT,               -- имя пассажира
    outbound       TEXT,               -- флаг направления (boolean → text для Greenplum)
    
    -- Технические атрибуты
    src_created_at_ts TIMESTAMP,        -- временная метка источника (для инкремента)
    load_dttm       TIMESTAMP NOT NULL, -- время загрузки в Greenplum
    batch_id        TEXT                -- идентификатор батча (из Airflow run_id)
)
DISTRIBUTED BY (ticket_no);            -- распределение по первичному ключу
```

**Решения по проектированию:**
- **DISTRIBUTED BY (ticket_no):** равномерное распределение, так как это первичный ключ
- **boolean → TEXT:** Greenplum хорошо работает с текстовым представлением для флагов
- **src_created_at_ts:** временная метка из даты связанного бронирования (см. раздел 7)

## 4. Проектирование внешней таблицы (PXF)

Используем существующий PXF-профиль для Postgres (как в `stg.bookings_ext`):

```sql
-- sql/stg/tickets_ddl.sql (продолжение)

DROP EXTERNAL TABLE IF EXISTS stg.tickets_ext CASCADE;

CREATE EXTERNAL TABLE stg.tickets_ext (
    ticket_no      TEXT,
    book_ref       TEXT,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT
)
LOCATION ('pxf://bookings-db:5432/demo?PROFILE=postgres&SERVER=bookings_db')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8';
```

**Решения:**
- Только бизнес-атрибуты во внешней таблице (без тех.колонок)
- PXF-профиль уже настроен через `bookings_db`

## 5. DDL-скрипт (создание таблиц)

Полный файл `sql/stg/tickets_ddl.sql`:

```sql
-- Внешняя таблица для доступа через PXF к bookings-db
DROP EXTERNAL TABLE IF EXISTS stg.tickets_ext CASCADE;

CREATE EXTERNAL TABLE stg.tickets_ext (
    ticket_no      TEXT,
    book_ref       TEXT,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT
)
LOCATION ('pxf://bookings-db:5432/demo?PROFILE=postgres&SERVER=bookings_db')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8';

-- Внутренняя таблица для хранения данных в Greenplum
DROP TABLE IF EXISTS stg.tickets CASCADE;

CREATE TABLE stg.tickets (
    ticket_no      TEXT NOT NULL,
    book_ref       TEXT NOT NULL,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT,
    
    src_created_at_ts TIMESTAMP,
    load_dttm       TIMESTAMP NOT NULL,
    batch_id        TEXT
)
DISTRIBUTED BY (ticket_no);
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
JOIN (
    -- Подзапрос: получаем book_date для инкремента
    -- Связываем tickets с bookings через внешнюю таблицу stg.bookings_ext
    SELECT 
        b.book_ref,
        b.book_date
    FROM stg.bookings_ext AS b_ext
    JOIN bookings.bookings AS b ON b_ext.book_ref = b.book_ref
    -- Берём только новые бронирования (по дате)
    WHERE b_ext.book_date > COALESCE(
        (
            SELECT max(src_created_at_ts)
            FROM stg.tickets
            WHERE batch_id <> '{{ run_id }}'::text
                OR batch_id IS NULL
        ),
        TIMESTAMP '1900-01-01 00:00:00'
    )
) AS b ON ext.book_ref = b.book_ref
AND NOT EXISTS (
    -- Защита от дубликатов в рамках одного батча
    SELECT 1
    FROM stg.tickets AS t
    WHERE t.batch_id = '{{ run_id }}'::text
        AND t.ticket_no = ext.ticket_no
);
```

**Объяснение логики:**
1. Из `stg.tickets_ext` берём все билеты
2. JOIN с подзапросом по `book_ref` — это даёт `book_date` из бронирования
3. Фильтр по `book_date > max(src_created_at_ts)` — берём только новые билеты
4. `NOT EXISTS` — защита от повторной загрузки того же билета в текущем батче

## 7. DQ-проверки (качество данных)

Скрипт `sql/stg/tickets_dq.sql`:

```sql
-- Проверка 1: совпадение количества билетов в источнике и STG
DO $$
DECLARE
    v_source_count BIGINT;
    v_stg_count BIGINT;
BEGIN
    -- Количество в источнике (новые билеты)
    SELECT COUNT(*) INTO v_source_count
    FROM (
        SELECT t.ticket_no
        FROM stg.tickets_ext AS t
        JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
        WHERE b.book_date > COALESCE(
            (SELECT max(src_created_at_ts) FROM stg.tickets 
             WHERE batch_id <> '{{ run_id }}'::text OR batch_id IS NULL),
            TIMESTAMP '1900-01-01 00:00:00'
        )
    ) AS source;
    
    -- Количество в STG (текущий батч)
    SELECT COUNT(*) INTO v_stg_count
    FROM stg.tickets
    WHERE batch_id = '{{ run_id }}'::text;
    
    -- Проверка
    IF v_source_count <> v_stg_count THEN
        RAISE EXCEPTION 'DQ FAILED: несовпадение количества билетов. Источник: %, STG: %', 
                          v_source_count, v_stg_count;
    END IF;
END $$;

-- Проверка 2: ссылочная целостность (все ticket_no должны иметь соответствующие book_ref)
SELECT 
    COUNT(*) AS orphan_tickets
FROM stg.tickets
WHERE batch_id = '{{ run_id }}'::text
    AND NOT EXISTS (
        SELECT 1
        FROM stg.bookings
        WHERE stg.bookings.book_ref = stg.tickets.book_ref
    );
-- Ожидаемое значение: 0

-- Проверка 3: отсутствие NULL в обязательных полях
SELECT 
    COUNT(*) AS null_tickets
FROM stg.tickets
WHERE batch_id = '{{ run_id }}'::text
    AND (ticket_no IS NULL OR book_ref IS NULL);
-- Ожидаемое значение: 0
```

## 8. Интеграция с существующим DAG

### Решение: расширить существующий DAG

**Почему не отдельный DAG:**
- `generate_bookings_day` уже есть в `bookings_to_gp_stage`
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

**Полный DAG (с обновлениями):**

```python
# ... (импорты и default_args без изменений)

with DAG(
    dag_id="bookings_to_gp_stage",
    # ... (параметры без изменений)
) as dag:
    generate_bookings_day = PostgresOperator(...)  # уже есть
    load_bookings_to_stg = PostgresOperator(...)   # уже есть
    check_row_counts = PostgresOperator(...)       # уже есть
    finish_summary = PythonOperator(...)           # уже есть

    # Новые задачи
    load_tickets_to_stg = PostgresOperator(
        task_id="load_tickets_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/tickets_load.sql",
    )

    check_tickets_dq = PostgresOperator(
        task_id="check_tickets_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/tickets_dq.sql",
    )

    # Обновлённые связи
    generate_bookings_day >> load_bookings_to_stg >> check_row_counts
    check_row_counts >> load_tickets_to_stg >> check_tickets_dq >> finish_summary
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
make gp-psql
```
```sql
-- внутри psql:
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
5. ⏳ Локальное тестирование (раздел 9)
6. ⏳ Проверка через Airflow UI
7. ⏳ Обновить `README.md` (добавить tickets в список STG-таблиц)

## 11. Сопутствующие изменения

После успешного тестирования обновить документацию:

**README.md:**
- Добавить `tickets` в список STG-таблиц
- Обновить описание DAG `bookings_to_gp_stage` (упомянуть загрузку билетов)

**educational-tasks.md:**
- Добавить задание по анализу `tickets` в STG
- Предложить построить витрину для анализа билетов (количество, выручка по направлениям)