-- DDL для ODS-слоя по таблице bookings (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации UPSERT/SCD1.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
CREATE TABLE IF NOT EXISTS ods.bookings (
    book_ref     TEXT NOT NULL,
    book_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    event_ts     TIMESTAMP,
    _load_id     TEXT NOT NULL,
    _load_ts     TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (book_ref);

COMMENT ON TABLE ods.bookings IS 'Бронирования (ODS).';
