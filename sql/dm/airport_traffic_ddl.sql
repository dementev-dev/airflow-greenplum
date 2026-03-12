-- DDL для витрины dm.airport_traffic (Пассажиропоток аэропортов).
--
-- Учебные цели:
-- 1. Обработка dual-role dimensions: один аэропорт выступает и как точка вылета,
--    и как точка прилета. Витрина собирает статистику в едином разрезе (date, airport).
-- 2. Денормализация: включение кода аэропорта (BK) и города для удобства анализа.
-- 3. Предупреждение о семантике данных: риск "двойного счета" выручки.
-- 4. Стратегия HWM по датам: пересчет только затронутых дней.

CREATE TABLE IF NOT EXISTS dm.airport_traffic (
    -- Зерно: дата и суррогатный ключ аэропорта
    traffic_date           DATE NOT NULL,
    airport_sk             INTEGER NOT NULL,

    -- Денормализованные атрибуты (из dim_airports)
    airport_bk             TEXT NOT NULL,
    city                   TEXT NOT NULL,

    -- Метрики вылета
    departures_flights     INTEGER NOT NULL DEFAULT 0,
    departures_passengers  INTEGER NOT NULL DEFAULT 0,
    departures_revenue     NUMERIC(15,2) NOT NULL DEFAULT 0,

    -- Метрики прилета
    arrivals_flights       INTEGER NOT NULL DEFAULT 0,
    arrivals_passengers    INTEGER NOT NULL DEFAULT 0,
    arrivals_revenue       NUMERIC(15,2) NOT NULL DEFAULT 0,

    -- Итоговые метрики
    total_passengers       INTEGER NOT NULL DEFAULT 0,

    -- Служебные поля
    created_at             TIMESTAMP NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP NOT NULL DEFAULT now(),
    _load_id               TEXT NOT NULL,
    _load_ts               TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false) -- Используем Heap для инкрементального UPSERT
DISTRIBUTED BY (airport_sk);

COMMENT ON TABLE dm.airport_traffic IS 'Витрина: ежедневный пассажиропоток аэропортов (UNION ALL, Heap)';

-- Комментарии к колонкам с предупреждением (учебная ценность)
COMMENT ON COLUMN dm.airport_traffic.departures_revenue IS 'Выручка от билетов, где аэропорт был точкой вылета. ВНИМАНИЕ: суммирование с arrivals_revenue приведет к двойному счету!';
COMMENT ON COLUMN dm.airport_traffic.arrivals_revenue IS 'Выручка от билетов, где аэропорт был точкой прилета. ВНИМАНИЕ: суммирование с departures_revenue приведет к двойному счету!';
