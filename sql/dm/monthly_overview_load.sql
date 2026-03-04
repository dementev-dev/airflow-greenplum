-- Загрузка витрины dm.monthly_overview: инкрементальный UPSERT по месяцам.
--
-- Учебные цели:
-- 1. Двухуровневая агрегация: чтобы честно посчитать среднюю заполняемость (avg_load_factor),
--    мы сначала считаем её для КАЖДОГО рейса, а затем берем среднее по месяцу.
-- 2. Ограничения SCD1: dim_airplanes — это SCD1-измерение. Мы берем total_seats из
--    его текущего состояния. Если бы самолет переоборудовали (изменили число мест) в прошлом,
--    для точного исторического расчета нам потребовалось бы SCD2-измерение.

-- Шаг 1: Находим затронутые месяцы (HWM).
CREATE TEMP TABLE tmp_monthly_affected_dates ON COMMIT DROP AS
SELECT DISTINCT cal.year_actual, cal.month_actual
FROM dds.fact_flight_sales f
JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
WHERE f._load_ts > (
    SELECT COALESCE(MAX(_load_ts), '1900-01-01'::TIMESTAMP)
    FROM dm.monthly_overview
);

-- Шаг 2: Уровень 1 - Агрегация фактов до рейса (flight_id).
-- Считаем точный load_factor для каждого отдельного перелета.
CREATE TEMP TABLE tmp_flight_level_metrics ON COMMIT DROP AS
SELECT
    cal.year_actual,
    cal.month_actual,
    f.flight_id,
    f.airplane_sk,
    a.total_seats, -- ВНИМАНИЕ: берется текущее значение (SCD1)
    COUNT(*) AS tickets_sold_per_flight,
    SUM(CASE WHEN f.is_boarded THEN 1 ELSE 0 END) AS boarded_per_flight,
    SUM(f.price) AS revenue_per_flight,
    -- Точный load factor конкретного рейса: посаженные пассажиры / кол-во мест
    (SUM(CASE WHEN f.is_boarded THEN 1 ELSE 0 END)::NUMERIC / NULLIF(a.total_seats, 0)) AS flight_load_factor
FROM dds.fact_flight_sales f
JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
JOIN dds.dim_airplanes a ON f.airplane_sk = a.airplane_sk
-- Фильтруем только затронутые месяцы
WHERE EXISTS (
    SELECT 1 FROM tmp_monthly_affected_dates tad
    WHERE tad.year_actual = cal.year_actual AND tad.month_actual = cal.month_actual
)
GROUP BY cal.year_actual, cal.month_actual, f.flight_id, f.airplane_sk, a.total_seats;

-- Шаг 3: Уровень 2 - Агрегация от рейсов до месяцев.
-- Плюс собираем уникальные метрики (пассажиры, маршруты) напрямую из фактов.
CREATE TEMP TABLE tmp_monthly_overview_delta ON COMMIT DROP AS
WITH flight_aggs AS (
    SELECT
        year_actual,
        month_actual,
        airplane_sk,
        COUNT(DISTINCT flight_id) AS total_flights,
        SUM(tickets_sold_per_flight) AS total_tickets,
        SUM(boarded_per_flight) AS total_boarded,
        SUM(revenue_per_flight) AS total_revenue,
        -- Честное среднее от рейсовых показателей
        AVG(flight_load_factor) AS avg_load_factor
    FROM tmp_flight_level_metrics
    GROUP BY year_actual, month_actual, airplane_sk
),
unique_aggs AS (
    SELECT
        cal.year_actual,
        cal.month_actual,
        f.airplane_sk,
        COUNT(DISTINCT r.route_bk) AS unique_routes, -- Считаем по BK (SCD2)
        COUNT(DISTINCT f.passenger_sk) AS unique_passengers
    FROM dds.fact_flight_sales f
    JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
    JOIN dds.dim_routes r ON f.route_sk = r.route_sk
    WHERE EXISTS (
        SELECT 1 FROM tmp_monthly_affected_dates tad
        WHERE tad.year_actual = cal.year_actual AND tad.month_actual = cal.month_actual
    )
    GROUP BY cal.year_actual, cal.month_actual, f.airplane_sk
)
SELECT
    fa.year_actual,
    fa.month_actual,
    fa.airplane_sk,
    a.airplane_bk,
    a.model AS airplane_model,
    a.total_seats,
    fa.total_flights,
    fa.total_tickets,
    fa.total_boarded,
    fa.total_revenue,
    (fa.total_revenue / NULLIF(fa.total_tickets, 0))::NUMERIC(10,2) AS avg_ticket_price,
    fa.avg_load_factor::NUMERIC(5,4) AS avg_load_factor,
    ua.unique_routes,
    ua.unique_passengers
FROM flight_aggs fa
JOIN unique_aggs ua ON fa.year_actual = ua.year_actual AND fa.month_actual = ua.month_actual AND fa.airplane_sk = ua.airplane_sk
JOIN dds.dim_airplanes a ON fa.airplane_sk = a.airplane_sk;

-- Шаг 4: UPDATE существующих записей.
UPDATE dm.monthly_overview AS tgt
SET
    airplane_bk       = src.airplane_bk,
    airplane_model    = src.airplane_model,
    total_seats       = src.total_seats,
    total_flights     = src.total_flights,
    total_tickets     = src.total_tickets,
    total_boarded     = src.total_boarded,
    total_revenue     = src.total_revenue,
    avg_ticket_price  = src.avg_ticket_price,
    avg_load_factor   = src.avg_load_factor,
    unique_routes     = src.unique_routes,
    unique_passengers = src.unique_passengers,
    updated_at        = now(),
    _load_id          = '{{ run_id }}',
    _load_ts          = now()
FROM tmp_monthly_overview_delta AS src
WHERE tgt.year_actual = src.year_actual
  AND tgt.month_actual = src.month_actual
  AND tgt.airplane_sk = src.airplane_sk
  AND (
      tgt.total_flights IS DISTINCT FROM src.total_flights
      OR tgt.total_revenue IS DISTINCT FROM src.total_revenue
      OR tgt.total_boarded IS DISTINCT FROM src.total_boarded
      OR tgt.unique_passengers IS DISTINCT FROM src.unique_passengers
      OR tgt.airplane_model IS DISTINCT FROM src.airplane_model
  );

-- Шаг 5: INSERT новых записей.
INSERT INTO dm.monthly_overview (
    year_actual,
    month_actual,
    airplane_sk,
    airplane_bk,
    airplane_model,
    total_seats,
    total_flights,
    total_tickets,
    total_boarded,
    total_revenue,
    avg_ticket_price,
    avg_load_factor,
    unique_routes,
    unique_passengers,
    _load_id
)
SELECT
    src.year_actual,
    src.month_actual,
    src.airplane_sk,
    src.airplane_bk,
    src.airplane_model,
    src.total_seats,
    src.total_flights,
    src.total_tickets,
    src.total_boarded,
    src.total_revenue,
    src.avg_ticket_price,
    src.avg_load_factor,
    src.unique_routes,
    src.unique_passengers,
    '{{ run_id }}' AS _load_id
FROM tmp_monthly_overview_delta AS src
WHERE NOT EXISTS (
    SELECT 1 FROM dm.monthly_overview AS tgt
    WHERE tgt.year_actual = src.year_actual
      AND tgt.month_actual = src.month_actual
      AND tgt.airplane_sk = src.airplane_sk
);
