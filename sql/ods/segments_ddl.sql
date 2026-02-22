-- DDL для ODS-слоя по таблице segments (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.segments (
    ticket_no       TEXT NOT NULL,
    flight_id       INTEGER NOT NULL,
    fare_conditions TEXT NOT NULL,
    segment_amount  NUMERIC(10,2),
    event_ts        TIMESTAMP,
    _load_id        TEXT NOT NULL,
    _load_ts        TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (ticket_no);
