-- DDL для слоя STG по таблице flights.
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.flights через PXF.
DROP EXTERNAL TABLE IF EXISTS stg.flights_ext;
CREATE EXTERNAL TABLE stg.flights_ext (
    flight_id            TEXT,
    route_no             TEXT,
    status               TEXT,
    scheduled_departure  TIMESTAMP,
    scheduled_arrival    TIMESTAMP,
    actual_departure     TIMESTAMP,
    actual_arrival       TIMESTAMP
)
LOCATION ('pxf://bookings.flights?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.flights — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.flights (
    flight_id            TEXT,
    route_no             TEXT,
    status               TEXT,
    scheduled_departure  TEXT,
    scheduled_arrival    TEXT,
    actual_departure     TEXT,
    actual_arrival       TEXT,
    src_created_at_ts    TIMESTAMP,
    load_dttm            TIMESTAMP NOT NULL DEFAULT now(),
    batch_id             TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: flight_id
-- Обоснование: flight_id — это уникальный идентификатор рейса.
-- Использование flight_id обеспечивает:
-- 1. Равномерное распределение данных по сегментам (flight_id имеет высокую кардинальность)
-- 2. Оптимизацию запросов, которые фильтруют или группируют по flight_id
-- Примечание: JOIN с таблицами, распределёнными по другим ключам, может требовать motion.
DISTRIBUTED BY (flight_id);
