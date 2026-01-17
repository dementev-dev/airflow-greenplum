-- DDL для слоя STG по таблице bookings.
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

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
-- Ключ распределения: book_ref
-- Обоснование: book_ref — это уникальный идентификатор бронирования.
-- Использование book_ref обеспечивает:
-- 1. Равномерное распределение данных по сегментам (book_ref имеет высокую кардинальность)
-- 2. Co-location данных bookings и tickets при JOIN по book_ref
-- 3. Оптимизацию запросов, которые фильтруют или группируют по book_ref
DISTRIBUTED BY (book_ref);

