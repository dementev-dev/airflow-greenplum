-- DDL для ODS-слоя по таблице airports (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Append-Only Row-oriented (zstd:1).
-- Обоснование: Используется паттерн TRUNCATE+INSERT (полный снимок).
-- Для узких таблиц Row-store производительнее Column-store при чтении всей строки.
CREATE TABLE IF NOT EXISTS ods.airports (
    airport_code TEXT NOT NULL,
    airport_name TEXT NOT NULL,
    city         TEXT NOT NULL,
    country      TEXT NOT NULL,
    coordinates  TEXT,
    timezone     TEXT NOT NULL,
    _load_id     TEXT NOT NULL,
    _load_ts     TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (airport_code);

COMMENT ON TABLE ods.airports IS 'Справочник аэропортов (ODS).';
