-- Загрузка витрины dm.airport_traffic: инкрементальный UPSERT по датам.
--
-- Учебные цели:
-- 1. Обработка dual-role dimensions через UNION ALL:
--    Мы превращаем один билет (факт) в два "события": вылет и прилет.
--    Это позволяет собрать единую статистику аэропорта в одном проходе.
-- 2. Инкремент по датам (HWM): в этой витрине метрики ограничены сутками,
--    поэтому пересчитываем только те дни, где появились новые факты.
-- 3. Агрегация рейсов: используем COUNT(DISTINCT flight_id) для подсчета рейсов.
-- 4. Денормализация (SCD1): обновление атрибутов (город) при изменениях в измерении.

-- Шаг 1: Находим даты, затронутые новыми/измененными фактами.
CREATE TEMP TABLE tmp_traffic_affected_dates ON COMMIT DROP AS
SELECT DISTINCT cal.date_actual AS traffic_date
FROM dds.fact_flight_sales f
JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
WHERE f._load_ts > (
    SELECT COALESCE(MAX(_load_ts), '1900-01-01'::TIMESTAMP)
    FROM dm.airport_traffic
);

-- Шаг 2: Unpivot (UNION ALL) и агрегация по затронутым датам.
CREATE TEMP TABLE tmp_airport_traffic_delta ON COMMIT DROP AS
WITH raw_events AS (
    -- Роль 1: Аэропорт вылета
    SELECT
        cal.date_actual AS traffic_date,
        f.departure_airport_sk AS airport_sk,
        f.flight_id,
        'departure' AS role,
        (CASE WHEN f.is_boarded THEN 1 ELSE 0 END) AS is_passenger,
        f.price AS revenue
    FROM dds.fact_flight_sales f
    JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
    WHERE cal.date_actual IN (SELECT traffic_date FROM tmp_traffic_affected_dates)

    UNION ALL

    -- Роль 2: Аэропорт прилета
    SELECT
        cal.date_actual AS traffic_date,
        f.arrival_airport_sk AS airport_sk,
        f.flight_id,
        'arrival' AS role,
        (CASE WHEN f.is_boarded THEN 1 ELSE 0 END) AS is_passenger,
        f.price AS revenue
    FROM dds.fact_flight_sales f
    JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
    WHERE cal.date_actual IN (SELECT traffic_date FROM tmp_traffic_affected_dates)
)
SELECT
    e.traffic_date,
    e.airport_sk,
    a.airport_bk, -- Берем из dim_airports.airport_bk
    a.city,
    -- Агрегация вылетов
    COUNT(DISTINCT CASE WHEN e.role = 'departure' THEN e.flight_id END) AS departures_flights,
    SUM(CASE WHEN e.role = 'departure' THEN e.is_passenger ELSE 0 END) AS departures_passengers,
    SUM(CASE WHEN e.role = 'departure' THEN e.revenue ELSE 0 END) AS departures_revenue,
    -- Агрегация прилетов
    COUNT(DISTINCT CASE WHEN e.role = 'arrival' THEN e.flight_id END) AS arrivals_flights,
    SUM(CASE WHEN e.role = 'arrival' THEN e.is_passenger ELSE 0 END) AS arrivals_passengers,
    SUM(CASE WHEN e.role = 'arrival' THEN e.revenue ELSE 0 END) AS arrivals_revenue,
    -- Итого: все пассажиры (сели + вышли в этом аэропорту)
    SUM(e.is_passenger) AS total_passengers
FROM raw_events e
JOIN dds.dim_airports a ON e.airport_sk = a.airport_sk
GROUP BY e.traffic_date, e.airport_sk, a.airport_bk, a.city;

-- Шаг 3: UPDATE существующих записей (включая денормализованные атрибуты).
UPDATE dm.airport_traffic AS tgt
SET
    airport_bk            = src.airport_bk,
    city                  = src.city,
    departures_flights    = src.departures_flights,
    departures_passengers = src.departures_passengers,
    departures_revenue    = src.departures_revenue,
    arrivals_flights      = src.arrivals_flights,
    arrivals_passengers   = src.arrivals_passengers,
    arrivals_revenue      = src.arrivals_revenue,
    total_passengers      = src.total_passengers,
    updated_at            = now(),
    _load_id              = '{{ run_id }}',
    _load_ts              = now()
FROM tmp_airport_traffic_delta AS src
WHERE tgt.traffic_date = src.traffic_date
  AND tgt.airport_sk   = src.airport_sk
  AND (
      tgt.total_passengers IS DISTINCT FROM src.total_passengers
      OR tgt.departures_flights IS DISTINCT FROM src.departures_flights
      OR tgt.arrivals_flights IS DISTINCT FROM src.arrivals_flights
      OR tgt.city IS DISTINCT FROM src.city
      OR tgt.airport_bk IS DISTINCT FROM src.airport_bk
  );

-- Шаг 4: INSERT новых записей.
INSERT INTO dm.airport_traffic (
    traffic_date,
    airport_sk,
    airport_bk,
    city,
    departures_flights,
    departures_passengers,
    departures_revenue,
    arrivals_flights,
    arrivals_passengers,
    arrivals_revenue,
    total_passengers,
    _load_id
)
SELECT
    src.traffic_date,
    src.airport_sk,
    src.airport_bk,
    src.city,
    src.departures_flights,
    src.departures_passengers,
    src.departures_revenue,
    src.arrivals_flights,
    src.arrivals_passengers,
    src.arrivals_revenue,
    src.total_passengers,
    '{{ run_id }}' AS _load_id
FROM tmp_airport_traffic_delta AS src
WHERE NOT EXISTS (
    SELECT 1 FROM dm.airport_traffic AS tgt
    WHERE tgt.traffic_date = src.traffic_date
      AND tgt.airport_sk   = src.airport_sk
);

ANALYZE dm.airport_traffic;
