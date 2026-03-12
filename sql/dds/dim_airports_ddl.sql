-- DDL для DDS-слоя по таблице dim_airports (SCD1-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации SCD1 UPSERT.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
CREATE TABLE IF NOT EXISTS dds.dim_airports (
    airport_sk   INTEGER NOT NULL,
    airport_bk   TEXT NOT NULL,
    airport_name TEXT NOT NULL,
    city         TEXT NOT NULL,
    country      TEXT NOT NULL,
    timezone     TEXT NOT NULL,
    coordinates  TEXT,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now(),
    _load_id     TEXT NOT NULL,
    _load_ts     TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (airport_sk);

COMMENT ON TABLE dds.dim_airports IS 'Измерение аэропортов (DDS).';
