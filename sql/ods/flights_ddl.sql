-- DDL для ODS-слоя по таблице flights (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.flights (
    flight_id           INTEGER NOT NULL,
    route_no            TEXT NOT NULL,
    status              TEXT NOT NULL,
    scheduled_departure TIMESTAMP WITH TIME ZONE,
    scheduled_arrival   TIMESTAMP WITH TIME ZONE,
    actual_departure    TIMESTAMP WITH TIME ZONE,
    actual_arrival      TIMESTAMP WITH TIME ZONE,
    event_ts            TIMESTAMP,
    _load_id            TEXT NOT NULL,
    _load_ts            TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (flight_id);
