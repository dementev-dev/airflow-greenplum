-- Создание внешней таблицы для доступа к bookings.tickets через PXF

DROP EXTERNAL TABLE IF EXISTS stg.tickets_ext CASCADE;

CREATE EXTERNAL TABLE stg.tickets_ext (
    ticket_no      TEXT,
    book_ref       TEXT,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT
)
LOCATION ('pxf://bookings-db:5432/demo?PROFILE=postgres&SERVER=bookings_db')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8';

-- Создание внутренней таблицы для хранения данных в Greenplum

DROP TABLE IF EXISTS stg.tickets CASCADE;

CREATE TABLE stg.tickets (
    ticket_no      TEXT NOT NULL,
    book_ref       TEXT NOT NULL,
    passenger_id   TEXT,
    passenger_name TEXT,
    outbound       TEXT,

    src_created_at_ts TIMESTAMP,
    load_dttm       TIMESTAMP NOT NULL,
    batch_id        TEXT
)
DISTRIBUTED BY (ticket_no);
