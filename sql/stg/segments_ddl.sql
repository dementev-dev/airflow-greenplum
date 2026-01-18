-- DDL для слоя STG по таблице segments.
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.segments через PXF.
DROP EXTERNAL TABLE IF EXISTS stg.segments_ext;
CREATE EXTERNAL TABLE stg.segments_ext (
    ticket_no        TEXT,
    flight_id        TEXT,
    fare_conditions  TEXT,
    price            NUMERIC(10,2)
)
LOCATION ('pxf://bookings.segments?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.segments — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.segments (
    ticket_no        TEXT,
    flight_id        TEXT,
    fare_conditions  TEXT,
    price            TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm        TIMESTAMP NOT NULL DEFAULT now(),
    batch_id         TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: ticket_no
-- Обоснование: ticket_no — это основной бизнес-ключ для билетов.
-- Использование ticket_no обеспечивает:
-- 1. Коллокацию данных segments и boarding_passes при JOIN по ticket_no
-- 2. Равномерное распределение данных по сегментам (ticket_no имеет высокую кардинальность)
-- Примечание: stg.tickets распределена по book_ref, поэтому JOIN segments ↔ tickets по ticket_no может требовать motion.
DISTRIBUTED BY (ticket_no);
