-- DDL для слоя STG по таблице airplanes (справочник).
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.airplanes_data через PXF.
-- PXF не поддерживает тип JSONB - используем TEXT для всех колонок.
DROP EXTERNAL TABLE IF EXISTS stg.airplanes_ext;
CREATE EXTERNAL TABLE stg.airplanes_ext (
    airplane_code TEXT,
    model         TEXT,
    range         TEXT,
    speed         TEXT
)
LOCATION ('pxf://bookings.airplanes_data?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.airplanes — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.airplanes (
    airplane_code      TEXT,
    model             TEXT,
    range             TEXT,
    speed             TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm         TIMESTAMP NOT NULL DEFAULT now(),
    batch_id          TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: airplane_code
-- Обоснование: airplane_code — это уникальный идентификатор самолёта.
-- Использование airplane_code обеспечивает:
-- 1. Равномерное распределение данных по сегментам (airplane_code имеет высокую кардинальность)
-- 2. Co-location данных airplanes и routes при JOIN по airplane_code
-- 3. Co-location данных airplanes и seats при JOIN по airplane_code
-- 4. Оптимизацию запросов, которые фильтруют или группируют по airplane_code
DISTRIBUTED BY (airplane_code);
