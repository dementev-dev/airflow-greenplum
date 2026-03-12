-- DDL для DDS-слоя по таблице dim_passengers (SCD1-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации SCD1 UPSERT.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
CREATE TABLE IF NOT EXISTS dds.dim_passengers (
    passenger_sk   INTEGER NOT NULL,
    passenger_id   TEXT NOT NULL,
    passenger_name TEXT NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP NOT NULL DEFAULT now(),
    _load_id       TEXT NOT NULL,
    _load_ts       TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (passenger_sk);

COMMENT ON TABLE dds.dim_passengers IS 'Измерение пассажиров (DDS).';
