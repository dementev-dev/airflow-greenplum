-- DDL для ODS-слоя по таблице tickets (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации UPSERT/SCD1.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
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
WITH (appendonly=false)
DISTRIBUTED BY (ticket_no);

COMMENT ON TABLE ods.tickets IS 'Билеты (ODS).';
