-- DDL для DDS-слоя по таблице dim_airplanes (SCD1-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации SCD1 UPSERT.
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
CREATE TABLE IF NOT EXISTS dds.dim_airplanes (
    airplane_sk INTEGER NOT NULL,
    airplane_bk TEXT NOT NULL,
    model       TEXT NOT NULL,
    range_km    INTEGER,
    speed_kmh   INTEGER,
    total_seats INTEGER,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now(),
    _load_id    TEXT NOT NULL,
    _load_ts    TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (airplane_sk);

COMMENT ON TABLE dds.dim_airplanes IS 'Измерение моделей самолетов (DDS).';
