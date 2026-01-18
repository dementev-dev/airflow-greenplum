-- DDL для слоя STG по таблице routes (справочник).
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.routes через PXF.
-- PXF не поддерживает типы TSTZRANGE, INTEGER[], TIME, INTERVAL - используем TEXT для всех колонок.
DROP EXTERNAL TABLE IF EXISTS stg.routes_ext;
CREATE EXTERNAL TABLE stg.routes_ext (
    route_no          TEXT,
    validity          TEXT,
    departure_airport TEXT,
    arrival_airport   TEXT,
    airplane_code     TEXT,
    days_of_week      TEXT,
    scheduled_time    TEXT,
    duration          TEXT
)
LOCATION ('pxf://bookings.routes?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.routes — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.routes (
    route_no          TEXT,
    validity          TEXT,
    departure_airport TEXT,
    arrival_airport   TEXT,
    airplane_code     TEXT,
    days_of_week      TEXT,
    scheduled_time    TEXT,
    duration          TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm         TIMESTAMP NOT NULL DEFAULT now(),
    batch_id          TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: route_no
-- Обоснование: route_no — бизнес-идентификатор маршрута и часто используется в фильтрах/джойнах.
-- Использование route_no обеспечивает:
-- 1. Равномерное распределение данных по сегментам (route_no имеет высокую кардинальность)
-- 2. Оптимизацию запросов, которые фильтруют или группируют по route_no
-- Примечание: JOIN по airport_code/airplane_code может требовать перераспределения данных (motion).
DISTRIBUTED BY (route_no);
