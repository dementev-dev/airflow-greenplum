-- DDL для слоя STG по таблице boarding_passes.
-- Используется как из общего скрипта ddl_gp.sql (через \i),
-- так и может выполняться отдельно при изменении схемы.

-- Схема stg для сырого слоя DWH.
CREATE SCHEMA IF NOT EXISTS stg;

-- Внешняя таблица в схеме stg для чтения данных из bookings.boarding_passes через PXF.
DROP EXTERNAL TABLE IF EXISTS stg.boarding_passes_ext;
CREATE EXTERNAL TABLE stg.boarding_passes_ext (
    ticket_no     TEXT,
    flight_id     TEXT,
    seat_no      TEXT,
    boarding_no   INTEGER,
    boarding_time TIMESTAMP
)
LOCATION ('pxf://bookings.boarding_passes?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- Внутренняя таблица stg.boarding_passes — сырой слой, все бизнес-колонки как TEXT.
CREATE TABLE IF NOT EXISTS stg.boarding_passes (
    ticket_no     TEXT,
    flight_id     TEXT,
    seat_no       TEXT,
    boarding_no   TEXT,
    boarding_time TEXT,
    src_created_at_ts TIMESTAMP,
    load_dttm     TIMESTAMP NOT NULL DEFAULT now(),
    batch_id      TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
-- Ключ распределения: ticket_no
-- Обоснование: ticket_no — это основной бизнес-ключ для билетов.
-- Использование ticket_no обеспечивает:
-- 1. Co-location данных boarding_passes и segments при JOIN по ticket_no
-- 2. Равномерное распределение данных по сегментам (ticket_no имеет высокую кардинальность)
-- Примечание: stg.tickets распределена по book_ref, поэтому JOIN boarding_passes ↔ tickets по ticket_no может требовать motion.
DISTRIBUTED BY (ticket_no);
