-- DDL для ODS-слоя по таблице segments (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации UPSERT/SCD1.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
CREATE TABLE IF NOT EXISTS ods.segments (
    ticket_no        TEXT NOT NULL,
    flight_id        INTEGER NOT NULL,
    fare_conditions  TEXT NOT NULL,
    amount           NUMERIC(10,2) NOT NULL,
    event_ts         TIMESTAMP,
    _load_id         TEXT NOT NULL,
    _load_ts         TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (ticket_no);

COMMENT ON TABLE ods.segments IS 'Сегменты перелета (ODS).';

