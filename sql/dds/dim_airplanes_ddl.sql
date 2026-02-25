-- DDL для DDS-слоя по таблице dim_airplanes (SCD1-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dim_airplanes (
    airplane_sk INTEGER NOT NULL,
    airplane_bk TEXT NOT NULL,
    model       TEXT NOT NULL,
    range_km    INTEGER,
    speed_kmh   INTEGER,
    total_seats INTEGER,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now(),
    _load_id    TEXT NOT NULL,
    _load_ts    TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (airplane_sk);
