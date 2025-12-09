-- Greenplum DDL (GPDB 6 совместимо)
-- Колонночная таблица (append-optimized) и распределение по ключу.
-- Внимание: append-optimized таблицы не поддерживают UNIQUE/PRIMARY KEY,
-- поэтому контроль дублей выполняем в DAG при загрузке.
CREATE TABLE IF NOT EXISTS public.orders (
    order_id    BIGINT,
    order_ts    TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
DISTRIBUTED BY (order_id);

-- Внешняя таблица для чтения данных из демо-БД bookings через PXF (JDBC).
-- Источник: таблица bookings.bookings в базе demo (Postgres, сервис bookings-db).
DROP EXTERNAL TABLE IF EXISTS public.ext_bookings_bookings;
CREATE EXTERNAL TABLE public.ext_bookings_bookings (
    book_ref     CHAR(6),
    book_date    TIMESTAMP,
    total_amount NUMERIC(10,2)
)
LOCATION ('pxf://bookings.bookings?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.bookings через PXF.
DROP EXTERNAL TABLE IF EXISTS stg.bookings_ext;
CREATE EXTERNAL TABLE stg.bookings_ext (
    book_ref     CHAR(6),
    book_date    TIMESTAMP,
    total_amount NUMERIC(10,2)
)
LOCATION ('pxf://bookings.bookings?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.bookings — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.bookings (
    book_ref          TEXT,
    book_date         TEXT,
    total_amount      TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm         TIMESTAMP NOT NULL DEFAULT now(),
    batch_id          TEXT NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
DISTRIBUTED BY (book_ref);
