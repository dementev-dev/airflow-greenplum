-- DDL для DDS-слоя по таблице dim_calendar (статическое измерение дат).

CREATE SCHEMA IF NOT EXISTS dds;

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
DISTRIBUTED BY (calendar_sk);
