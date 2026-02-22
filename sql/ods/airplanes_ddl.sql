-- DDL для ODS-слоя по таблице airplanes (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.airplanes (
    airplane_code TEXT NOT NULL,
    model         TEXT NOT NULL,
    range_km      INTEGER,
    speed_kmh     INTEGER,
    _load_id      TEXT NOT NULL,
    _load_ts      TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (airplane_code);
