-- DDL для DM-слоя: витрина sales_report.
--
-- Бизнес-вопрос: "Какова выручка, кол-во билетов и boarding rate
-- по направлениям/тарифам за каждый день?"
--
-- Паттерны для студентов:
-- - Денормализация измерений (города, аэропорты, тарифы) для удобства аналитики
-- - Служебные поля календаря (day_of_week, day_name, is_weekend)
-- - Heap-таблица с UPDATE (нужен для UPSERT)

CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.sales_report (
    -- Ключ (зерно витрины)
    flight_date            DATE NOT NULL,
    departure_airport_sk   INTEGER NOT NULL,
    arrival_airport_sk     INTEGER NOT NULL,
    tariff_sk              INTEGER NOT NULL,

    -- Денормализованные атрибуты (для удобства аналитики)
    departure_city         TEXT NOT NULL,
    departure_airport_bk   TEXT NOT NULL,
    arrival_city           TEXT NOT NULL,
    arrival_airport_bk     TEXT NOT NULL,
    fare_conditions        TEXT NOT NULL,

    -- Атрибуты календаря
    day_of_week            INTEGER NOT NULL,
    day_name               TEXT NOT NULL,
    is_weekend             BOOLEAN NOT NULL,

    -- Метрики
    tickets_sold           INTEGER NOT NULL,
    passengers_boarded     INTEGER NOT NULL,
    total_revenue          NUMERIC(15,2) NOT NULL,
    avg_price              NUMERIC(10,2) NOT NULL,
    min_price              NUMERIC(10,2),
    max_price              NUMERIC(10,2),
    boarding_rate          NUMERIC(5,4) NOT NULL,  -- boarded / sold

    -- Служебные поля (канон из naming_conventions.md)
    created_at             TIMESTAMP NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP NOT NULL DEFAULT now(),
    _load_id               TEXT NOT NULL,
    _load_ts               TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (flight_date);

-- Комментарии для документирования
COMMENT ON TABLE dm.sales_report IS
    'Витрина продаж: выручка, билеты и boarding rate по направлениям/тарифам/дням';

COMMENT ON COLUMN dm.sales_report.boarding_rate IS
    'Доля пассажиров, прошедших посадку (passengers_boarded / tickets_sold)';
