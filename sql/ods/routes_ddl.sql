-- DDL для ODS-слоя по таблице routes (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.routes (
    route_no          TEXT NOT NULL,
    validity          TEXT NOT NULL,
    departure_airport TEXT NOT NULL,
    arrival_airport   TEXT NOT NULL,
    airplane_code     TEXT NOT NULL,
    days_of_week      TEXT,
    departure_time    TIME,
    duration          INTERVAL,
    _load_id          TEXT NOT NULL,
    _load_ts          TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (route_no);
