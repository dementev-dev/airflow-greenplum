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
DISTRIBUTED BY (ticket_no);
