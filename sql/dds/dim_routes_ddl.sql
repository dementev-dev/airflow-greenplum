-- DDL для DDS-слоя по таблице dim_routes (SCD2-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dim_routes (
    route_sk          INTEGER NOT NULL,
    route_bk          TEXT NOT NULL,
    departure_airport TEXT NOT NULL,
    arrival_airport   TEXT NOT NULL,
    airplane_code     TEXT NOT NULL,
    days_of_week      TEXT,
    departure_time    TIME,
    duration          INTERVAL,
    hashdiff          TEXT NOT NULL,
    valid_from        DATE NOT NULL,
    valid_to          DATE,
    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    updated_at        TIMESTAMP NOT NULL DEFAULT now(),
    _load_id          TEXT NOT NULL,
    _load_ts          TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (route_sk);
