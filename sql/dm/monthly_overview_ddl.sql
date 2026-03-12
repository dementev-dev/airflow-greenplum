-- DDL для витрины dm.monthly_overview (Помесячная сводка).
--
-- Учебные цели:
-- 1. Двухуровневая агрегация (до рейса, затем до месяца) для точного расчета
--    средних показателей (load factor), избегая парадокса Симпсона.
-- 2. Выбор ключа распределения (Distribution Key):
--    Мы используем airplane_sk, а НЕ (year, month). Распределение по датам в MPP — 
--    это антипаттерн, приводящий к Data Skew (весь месяц на одном сегменте).

CREATE TABLE IF NOT EXISTS dm.monthly_overview (
    -- Зерно: Месяц и самолет
    year_actual            INTEGER NOT NULL,
    month_actual           INTEGER NOT NULL,
    airplane_sk            INTEGER NOT NULL,

    -- Денормализованные атрибуты
    airplane_bk            TEXT NOT NULL,
    airplane_model         TEXT NOT NULL,
    total_seats            INTEGER NOT NULL,

    -- Метрики
    total_flights          INTEGER NOT NULL DEFAULT 0,
    total_tickets          INTEGER NOT NULL DEFAULT 0,
    total_boarded          INTEGER NOT NULL DEFAULT 0,
    total_revenue          NUMERIC(15,2) NOT NULL DEFAULT 0,
    avg_ticket_price       NUMERIC(10,2),
    avg_load_factor        NUMERIC(5,4),         -- Точный load factor (сначала по рейсу, потом среднее)
    unique_routes          INTEGER NOT NULL DEFAULT 0,
    unique_passengers      INTEGER NOT NULL DEFAULT 0,

    -- Служебные
    created_at             TIMESTAMP NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP NOT NULL DEFAULT now(),
    _load_id               TEXT NOT NULL,
    _load_ts               TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false) -- Heap для UPSERT
DISTRIBUTED BY (airplane_sk); -- Избегаем распределения по (year, month)

COMMENT ON TABLE dm.monthly_overview IS 'Витрина: помесячная аналитика с точным load_factor (Двухуровневая агрегация, Heap)';
