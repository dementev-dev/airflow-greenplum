-- DDL для ODS-слоя по таблице seats (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.seats (
    airplane_code   TEXT NOT NULL,
    seat_no         TEXT NOT NULL,
    fare_conditions TEXT NOT NULL,
    _load_id        TEXT NOT NULL,
    _load_ts        TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (airplane_code);
