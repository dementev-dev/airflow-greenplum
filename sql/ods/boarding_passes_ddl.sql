-- DDL для ODS-слоя по таблице boarding_passes (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации UPSERT/SCD1.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
CREATE TABLE IF NOT EXISTS ods.boarding_passes (
    ticket_no     TEXT NOT NULL,
    flight_id     INTEGER NOT NULL,
    seat_no       TEXT NOT NULL,
    boarding_no   INTEGER,
    boarding_time TIMESTAMP WITH TIME ZONE,
    event_ts      TIMESTAMP,
    _load_id      TEXT NOT NULL,
    _load_ts      TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (ticket_no);

COMMENT ON TABLE ods.boarding_passes IS 'Посадочные талоны (ODS).';
