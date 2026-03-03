-- DDL для DDS-слоя по таблице dim_calendar (статическое измерение дат).

CREATE SCHEMA IF NOT EXISTS dds;

-- Тип таблицы: Append-Only Row-oriented (zstd:1).
-- Обоснование: Статичные данные без обновлений. Обеспечивает эффективное сжатие.
CREATE TABLE IF NOT EXISTS dds.dim_calendar (
    calendar_sk  INTEGER NOT NULL,
    date_actual  DATE NOT NULL,
    year_actual  INTEGER NOT NULL,
    month_actual INTEGER NOT NULL,
    day_actual   INTEGER NOT NULL,
    day_of_week  INTEGER NOT NULL,
    day_name     TEXT NOT NULL,
    is_weekend   BOOLEAN NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (calendar_sk);

COMMENT ON TABLE dds.dim_calendar IS 'Измерение календаря (DDS).';
