-- DDL для DDS-слоя по таблице dim_tariffs (SCD1-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dim_tariffs (
    tariff_sk       INTEGER NOT NULL,
    fare_conditions TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now(),
    _load_id        TEXT NOT NULL,
    _load_ts        TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (tariff_sk);
