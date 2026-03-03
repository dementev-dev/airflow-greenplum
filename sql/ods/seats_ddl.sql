-- DDL для ODS-слоя по таблице seats (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Append-Only Row-oriented (zstd:1).
-- Обоснование: Используется паттерн TRUNCATE+INSERT (полный снимок).
-- Для узких таблиц Row-store производительнее Column-store при чтении всей строки.
CREATE TABLE IF NOT EXISTS ods.seats (
    airplane_code    TEXT NOT NULL,
    seat_no          TEXT NOT NULL,
    fare_conditions  TEXT NOT NULL,
    _load_id         TEXT NOT NULL,
    _load_ts         TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (airplane_code);

COMMENT ON TABLE ods.seats IS 'Справочник мест в самолетах (ODS).';
