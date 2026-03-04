-- Загрузка витрины dm.passenger_loyalty: инкрементальный UPSERT по затронутым ключам.
--
-- Учебные цели:
-- 1. Метод "затронутых ключей": HWM по _load_ts находит затронутые ID пассажиров,
--    а затем мы ПЕРЕСЧИТЫВАЕМ всю историю именно для этого круга лиц.
--    Это гарантирует точность накопительных агрегатов (total_spent, dates).
-- 2. Использование DISTINCT ON (PostgreSQL-специфика): самый лаконичный способ
--    найти "самое частое" (моду) в рамках группы.
-- 3. Обработка NULL в фактах: фильтрация (passenger_sk IS NOT NULL).
-- 4. Агрегация SCD2-измерений: при подсчете уникальных маршрутов (dim_routes)
--    нужно агрегировать по BK (route_bk), т.к. один маршрут может иметь несколько SK (версий).

-- Шаг 1: Находим ID пассажиров, чьи данные изменились или добавились в фактах.
CREATE TEMP TABLE tmp_loyalty_affected_keys ON COMMIT DROP AS
SELECT DISTINCT passenger_sk
FROM dds.fact_flight_sales
WHERE _load_ts > (
    SELECT COALESCE(MAX(_load_ts), '1900-01-01'::TIMESTAMP)
    FROM dm.passenger_loyalty
)
AND passenger_sk IS NOT NULL;

-- Шаг 2: Для затронутых лиц считаем ИТОГОВЫЕ агрегаты по ВСЕЙ истории фактов.
CREATE TEMP TABLE tmp_passenger_delta ON COMMIT DROP AS
WITH base_metrics AS (
    -- Агрегируем количественные метрики.
    -- JOIN dds.dim_routes нужен для подсчета УНИКАЛЬНЫХ маршрутов по BK (т.к. dim_routes - SCD2).
    SELECT
        f.passenger_sk,
        COUNT(DISTINCT f.book_ref) AS total_bookings,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN f.is_boarded THEN 1 ELSE 0 END) AS total_boarded,
        SUM(f.price) AS total_spent,
        COUNT(DISTINCT r.route_bk) AS unique_routes, -- Агрегация по BK (бизнес-ключу) маршрута
        MIN(cal.date_actual) AS first_flight_date,
        MAX(cal.date_actual) AS last_flight_date
    FROM dds.fact_flight_sales f
    JOIN dds.dim_calendar cal ON f.calendar_sk = cal.calendar_sk
    JOIN dds.dim_routes r ON f.route_sk = r.route_sk
    WHERE f.passenger_sk IN (SELECT passenger_sk FROM tmp_loyalty_affected_keys)
    GROUP BY f.passenger_sk
),
fare_modes AS (
    -- Находим самый частый класс обслуживания для каждого пассажира.
    -- DISTINCT ON при ничьей выбирает произвольный вариант из топ-результатов.
    SELECT DISTINCT ON (f.passenger_sk)
        f.passenger_sk,
        tar.fare_conditions AS favorite_fare_conditions
    FROM dds.fact_flight_sales f
    JOIN dds.dim_tariffs tar ON f.tariff_sk = tar.tariff_sk
    WHERE f.passenger_sk IN (SELECT passenger_sk FROM tmp_loyalty_affected_keys)
    GROUP BY f.passenger_sk, tar.fare_conditions
    ORDER BY f.passenger_sk, COUNT(*) DESC
)
SELECT
    bm.*,
    fm.favorite_fare_conditions,
    p.passenger_id AS passenger_bk, -- Исправлено: в dim_passengers BK называется passenger_id
    p.passenger_name,
    (bm.last_flight_date - bm.first_flight_date) AS days_as_customer
FROM base_metrics bm
JOIN dds.dim_passengers p ON bm.passenger_sk = p.passenger_sk
JOIN fare_modes fm ON bm.passenger_sk = fm.passenger_sk;

-- Шаг 3: UPDATE существующих записей.
UPDATE dm.passenger_loyalty AS tgt
SET
    passenger_name           = src.passenger_name,
    total_bookings           = src.total_bookings,
    total_flights            = src.total_flights,
    total_boarded            = src.total_boarded,
    total_spent              = src.total_spent,
    avg_ticket_price         = (src.total_spent / NULLIF(src.total_flights, 0))::NUMERIC(10,2),
    favorite_fare_conditions = src.favorite_fare_conditions,
    unique_routes            = src.unique_routes,
    first_flight_date        = src.first_flight_date,
    last_flight_date         = src.last_flight_date,
    days_as_customer         = src.days_as_customer,
    updated_at               = now(),
    _load_id                 = '{{ run_id }}',
    _load_ts                 = now()
FROM tmp_passenger_delta AS src
WHERE tgt.passenger_sk = src.passenger_sk
  AND (
      -- Обновляем только если что-то реально изменилось
      tgt.total_flights    IS DISTINCT FROM src.total_flights
      OR tgt.total_boarded IS DISTINCT FROM src.total_boarded
      OR tgt.total_spent   IS DISTINCT FROM src.total_spent
      OR tgt.last_flight_date IS DISTINCT FROM src.last_flight_date
      OR tgt.passenger_name IS DISTINCT FROM src.passenger_name
  );

-- Шаг 4: INSERT новых пассажиров.
INSERT INTO dm.passenger_loyalty (
    passenger_sk,
    passenger_bk,
    passenger_name,
    total_bookings,
    total_flights,
    total_boarded,
    total_spent,
    avg_ticket_price,
    favorite_fare_conditions,
    unique_routes,
    first_flight_date,
    last_flight_date,
    days_as_customer,
    _load_id
)
SELECT
    src.passenger_sk,
    src.passenger_bk,
    src.passenger_name,
    src.total_bookings,
    src.total_flights,
    src.total_boarded,
    src.total_spent,
    (src.total_spent / NULLIF(src.total_flights, 0))::NUMERIC(10,2),
    src.favorite_fare_conditions,
    src.unique_routes,
    src.first_flight_date,
    src.last_flight_date,
    src.days_as_customer,
    '{{ run_id }}' AS _load_id
FROM tmp_passenger_delta AS src
WHERE NOT EXISTS (
    SELECT 1 FROM dm.passenger_loyalty AS tgt
    WHERE tgt.passenger_sk = src.passenger_sk
);
