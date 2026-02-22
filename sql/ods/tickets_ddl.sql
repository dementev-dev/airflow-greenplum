-- DDL для ODS-слоя по таблице tickets (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.tickets (
    ticket_no      TEXT NOT NULL,
    book_ref       TEXT NOT NULL,
    passenger_id   TEXT NOT NULL,
    passenger_name TEXT NOT NULL,
    is_outbound    BOOLEAN,
    event_ts       TIMESTAMP,
    _load_id       TEXT NOT NULL,
    _load_ts       TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (ticket_no);
