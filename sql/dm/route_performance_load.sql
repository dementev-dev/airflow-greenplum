-- Загрузка витрины dm.route_performance: Full Rebuild.
--
-- Учебные цели:
-- 1. Демонстрация TRUNCATE + INSERT. Таблица маленькая (~1000 строк),
--    пересоздать её с нуля дешевле, чем вычислять дельту.
-- 2. Обработка SCD2: агрегируем факты по бизнес-ключу route_bk,
--    чтобы собрать данные со всех исторических версий маршрута.
-- 3. Денормализация атрибутов из ТЕКУЩЕЙ (актуальной) версии измерения.

-- Шаг 1: Очистка таблицы (AO Column Store не поддерживает эффективный DELETE/UPDATE).
TRUNCATE dm.route_performance;

-- Шаг 2: Агрегация всех фактов по бизнес-ключу маршрута.
-- Используем временную таблицу для удобства и читаемости.
CREATE TEMP TABLE tmp_route_metrics ON COMMIT DROP AS
SELECT
    r.route_bk,
    COUNT(DISTINCT f.flight_id) AS total_flights,
    COUNT(*) AS total_tickets,
    SUM(CASE WHEN f.is_boarded THEN 1 ELSE 0 END) AS total_boarded,
    SUM(f.price) AS total_revenue,
    MIN(cal.date_actual) AS first_flight_date,
    MAX(cal.date_actual) AS last_flight_date,
    -- Доля посаженных пассажиров среди всех проданных билетов маршрута
    -- (упрощение: не взвешено по рейсам)
    AVG(CASE WHEN f.is_boarded THEN 1 ELSE 0 END::NUMERIC) AS avg_boarding_rate
FROM dds.fact_flight_sales f
JOIN dds.dim_routes r ON f.route_sk = r.route_sk
JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
GROUP BY r.route_bk;

-- Шаг 3: Объединяем агрегаты с АКТУАЛЬНЫМИ атрибутами маршрута, аэропортов и самолетов.
INSERT INTO dm.route_performance (
    route_bk,
    route_sk,
    departure_airport_bk,
    departure_city,
    arrival_airport_bk,
    arrival_city,
    airplane_bk,
    airplane_model,
    total_seats,
    total_flights,
    total_tickets,
    total_boarded,
    total_revenue,
    avg_ticket_price,
    avg_boarding_rate,
    avg_load_factor,
    first_flight_date,
    last_flight_date,
    _load_id
)
SELECT
    m.route_bk,
    r_curr.route_sk,
    dep.airport_bk AS departure_airport_bk,
    dep.city AS departure_city,
    arr.airport_bk AS arrival_airport_bk,
    arr.city AS arrival_city,
    air.airplane_bk,
    air.model AS airplane_model,
    air.total_seats,
    m.total_flights,
    m.total_tickets,
    m.total_boarded,
    m.total_revenue,
    -- Средний чек
    (m.total_revenue / NULLIF(m.total_tickets, 0))::NUMERIC(10,2),
    m.avg_boarding_rate::NUMERIC(5,4),
    -- Load Factor: общее кол-во посадок / (кол-во рейсов * мест в самолете)
    ROUND(
        m.total_boarded::NUMERIC / NULLIF(m.total_flights * air.total_seats, 0),
        4
    ) AS avg_load_factor,
    m.first_flight_date,
    m.last_flight_date,
    '{{ run_id }}' AS _load_id
FROM tmp_route_metrics m
JOIN dds.dim_routes r_curr ON m.route_bk = r_curr.route_bk AND r_curr.valid_to IS NULL
JOIN dds.dim_airports dep ON r_curr.departure_airport = dep.airport_bk
JOIN dds.dim_airports arr ON r_curr.arrival_airport = arr.airport_bk
JOIN dds.dim_airplanes air ON r_curr.airplane_code = air.airplane_bk;
