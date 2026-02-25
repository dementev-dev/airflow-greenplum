-- DDL для DDS-слоя по таблице dim_passengers (SCD1-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dim_passengers (
    passenger_sk   INTEGER NOT NULL,
    passenger_bk   TEXT NOT NULL,
    passenger_name TEXT NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP NOT NULL DEFAULT now(),
    _load_id       TEXT NOT NULL,
    _load_ts       TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (passenger_sk);
