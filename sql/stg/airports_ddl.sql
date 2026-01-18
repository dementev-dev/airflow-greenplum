-- DDL для слоя STG по таблице airports (справочник).
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.airports_data через PXF.
-- PXF не поддерживает типы JSONB, POINT - используем TEXT для всех колонок.
DROP EXTERNAL TABLE IF EXISTS stg.airports_ext;
CREATE EXTERNAL TABLE stg.airports_ext (
    airport_code TEXT,
    airport_name  TEXT,
    city          TEXT,
    country       TEXT,
    coordinates   TEXT,
    timezone      TEXT
)
LOCATION ('pxf://bookings.airports_data?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.airports — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.airports (
    airport_code      TEXT,
    airport_name      TEXT,
    city              TEXT,
    country           TEXT,
    coordinates       TEXT,
    timezone          TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm         TIMESTAMP NOT NULL DEFAULT now(),
    batch_id          TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: airport_code
-- Обоснование: airport_code — это уникальный идентификатор аэропорта.
-- Использование airport_code обеспечивает:
-- 1. Равномерное распределение данных по сегментам (airport_code имеет высокую кардинальность)
-- 2. Co-location данных airports и routes при JOIN по departure_airport/arrival_airport
-- 3. Оптимизацию запросов, которые фильтруют или группируют по airport_code
DISTRIBUTED BY (airport_code);
