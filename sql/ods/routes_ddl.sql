-- DDL для ODS-слоя по таблице routes (текущее состояние, SCD1).

CREATE SCHEMA IF NOT EXISTS ods;

-- Тип таблицы: Append-Only Row-oriented (zstd:1).
-- Обоснование: Используется паттерн TRUNCATE+INSERT (полный снимок).
-- Для узких таблиц Row-store производительнее Column-store при чтении всей строки.
CREATE TABLE IF NOT EXISTS ods.routes (
    route_no          TEXT NOT NULL,
    validity          TEXT NOT NULL,
    departure_airport TEXT NOT NULL,
    arrival_airport   TEXT NOT NULL,
    airplane_code     TEXT NOT NULL,
    days_of_week      INTEGER[],
    departure_time    TIME NOT NULL,
    duration          INTERVAL NOT NULL,
    _load_id          TEXT NOT NULL,
    _load_ts          TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (route_no, validity);

COMMENT ON TABLE ods.routes IS 'Справочник маршрутов (ODS).';
