-- Загрузка DM витрины sales_report: инкрементальный UPSERT.
--
-- Паттерны для студентов:
-- - Использование TEMP TABLE: агрегация считается только ОДИН раз (канон для MPP)
-- - Инкрементальность (HWM — High-Water Mark): витрина сравнивает свой MAX(_load_ts)
--   с _load_ts фактов в DDS и пересчитывает агрегаты только для затронутых дат.
--   Это делает конвейер самовосстанавливающимся: если DAG не запускался несколько дней,
--   при следующем запуске витрина автоматически «догонит» всю накопленную дельту.
-- - Денормализация: города и названия тарифов тащим в витрину
-- - UPSERT: UPDATE изменившихся + INSERT новых (нужен heap для UPDATE)
-- - IS DISTINCT FROM для корректного сравнения NULL

-- Шаг 1: Считаем агрегаты для дельты (новых данных с момента последнего обновления витрины).
-- ON COMMIT DROP гарантирует, что таблица исчезнет после завершения транзакции (Airflow сессии).
CREATE TEMP TABLE tmp_sales_report_delta ON COMMIT DROP AS
SELECT
    cal.date_actual AS flight_date,
    dep.airport_sk AS departure_airport_sk,
    arr.airport_sk AS arrival_airport_sk,
    tar.tariff_sk,

    -- Денормализованные атрибуты
    dep.city AS departure_city,
    dep.airport_bk AS departure_airport_bk,
    arr.city AS arrival_city,
    arr.airport_bk AS arrival_airport_bk,
    tar.fare_conditions,

    -- Атрибуты календаря
    cal.day_of_week,
    cal.day_name,
    cal.is_weekend,

    -- Метрики
    COUNT(*) AS tickets_sold,
    SUM(CASE WHEN f.is_boarded THEN 1 ELSE 0 END) AS passengers_boarded,
    SUM(f.price) AS total_revenue,
    AVG(f.price) AS avg_price,
    MIN(f.price) AS min_price,
    MAX(f.price) AS max_price,
    -- boarding_rate: делим boarded на sold с защитой от деления на 0
    ROUND(
        SUM(CASE WHEN f.is_boarded THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0),
        4
    ) AS boarding_rate

FROM dds.fact_flight_sales AS f
JOIN dds.dim_calendar AS cal
    ON cal.calendar_sk = f.calendar_sk
JOIN dds.dim_airports AS dep
    ON dep.airport_sk = f.departure_airport_sk
JOIN dds.dim_airports AS arr
    ON arr.airport_sk = f.arrival_airport_sk
JOIN dds.dim_tariffs AS tar
    ON tar.tariff_sk = f.tariff_sk
WHERE cal.date_actual IN (
    -- ИНКРЕМЕНТАЛЬНЫЙ ФИЛЬТР (HWM — High-Water Mark):
    -- Ищем даты полётов, в которых появились новые или изменённые факты
    -- с момента последнего обновления витрины (MAX(_load_ts) в dm.sales_report).
    -- Если витрина пуста — '1900-01-01' заберёт всю историю (первичная загрузка).
    SELECT DISTINCT cal_sq.date_actual
    FROM dds.fact_flight_sales AS f_sq
    JOIN dds.dim_calendar AS cal_sq ON f_sq.calendar_sk = cal_sq.calendar_sk
    WHERE f_sq._load_ts > (
        SELECT COALESCE(MAX(_load_ts), '1900-01-01'::TIMESTAMP)
        FROM dm.sales_report
    )
)
GROUP BY
    cal.date_actual,
    dep.airport_sk, dep.city, dep.airport_bk,
    arr.airport_sk, arr.city, arr.airport_bk,
    tar.tariff_sk, tar.fare_conditions,
    cal.day_of_week, cal.day_name, cal.is_weekend
DISTRIBUTED BY (departure_airport_sk, arrival_airport_sk);

-- Шаг 2: UPDATE существующих строк.
-- Обновляем метрики и денормализованные атрибуты.
UPDATE dm.sales_report AS tgt
SET
    departure_city     = src.departure_city,
    arrival_city       = src.arrival_city,
    fare_conditions    = src.fare_conditions,
    day_of_week        = src.day_of_week,
    day_name           = src.day_name,
    is_weekend         = src.is_weekend,
    tickets_sold       = src.tickets_sold,
    passengers_boarded = src.passengers_boarded,
    total_revenue      = src.total_revenue,
    avg_price          = src.avg_price,
    min_price          = src.min_price,
    max_price          = src.max_price,
    boarding_rate      = src.boarding_rate,
    updated_at         = now(),
    _load_id           = '{{ run_id }}',
    _load_ts           = now()
FROM tmp_sales_report_delta AS src
WHERE tgt.flight_date          = src.flight_date
  AND tgt.departure_airport_sk = src.departure_airport_sk
  AND tgt.arrival_airport_sk   = src.arrival_airport_sk
  AND tgt.tariff_sk            = src.tariff_sk
  AND (
      -- Обновляем только если что-то реально изменилось
      tgt.tickets_sold       IS DISTINCT FROM src.tickets_sold
      OR tgt.passengers_boarded IS DISTINCT FROM src.passengers_boarded
      OR tgt.total_revenue   IS DISTINCT FROM src.total_revenue
      OR tgt.boarding_rate   IS DISTINCT FROM src.boarding_rate
      OR tgt.departure_city  IS DISTINCT FROM src.departure_city
      OR tgt.arrival_city    IS DISTINCT FROM src.arrival_city
  );

-- Шаг 3: INSERT новых строк (те, которых нет по составному ключу).
INSERT INTO dm.sales_report (
    flight_date,
    departure_airport_sk,
    arrival_airport_sk,
    tariff_sk,
    departure_city,
    departure_airport_bk,
    arrival_city,
    arrival_airport_bk,
    fare_conditions,
    day_of_week,
    day_name,
    is_weekend,
    tickets_sold,
    passengers_boarded,
    total_revenue,
    avg_price,
    min_price,
    max_price,
    boarding_rate,
    _load_id
)
SELECT
    src.flight_date,
    src.departure_airport_sk,
    src.arrival_airport_sk,
    src.tariff_sk,
    src.departure_city,
    src.departure_airport_bk,
    src.arrival_city,
    src.arrival_airport_bk,
    src.fare_conditions,
    src.day_of_week,
    src.day_name,
    src.is_weekend,
    src.tickets_sold,
    src.passengers_boarded,
    src.total_revenue,
    src.avg_price,
    src.min_price,
    src.max_price,
    src.boarding_rate,
    '{{ run_id }}' AS _load_id
FROM tmp_sales_report_delta AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM dm.sales_report AS tgt
    WHERE tgt.flight_date          = src.flight_date
      AND tgt.departure_airport_sk = src.departure_airport_sk
      AND tgt.arrival_airport_sk   = src.arrival_airport_sk
      AND tgt.tariff_sk            = src.tariff_sk
);
