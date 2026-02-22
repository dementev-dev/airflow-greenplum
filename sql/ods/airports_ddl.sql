-- DDL для ODS-слоя по таблице airports (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

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
DISTRIBUTED BY (airport_code);
