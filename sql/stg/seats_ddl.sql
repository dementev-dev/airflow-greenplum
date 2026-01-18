-- DDL для слоя STG по таблице seats (справочник).
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.seats через PXF.
DROP EXTERNAL TABLE IF EXISTS stg.seats_ext;
CREATE EXTERNAL TABLE stg.seats_ext (
    airplane_code    TEXT,
    seat_no         TEXT,
    fare_conditions TEXT
)
LOCATION ('pxf://bookings.seats?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.seats — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.seats (
    airplane_code      TEXT,
    seat_no           TEXT,
    fare_conditions   TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm         TIMESTAMP NOT NULL DEFAULT now(),
    batch_id          TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: airplane_code
-- Обоснование: airplane_code обеспечивает коллокацию seats ↔ airplanes при JOIN по airplane_code
-- (в MPP это уменьшает вероятность перераспределения данных / motion).
DISTRIBUTED BY (airplane_code);
