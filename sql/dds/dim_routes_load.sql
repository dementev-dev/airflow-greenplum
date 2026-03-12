-- Загрузка DDS dim_routes: SCD2 с hashdiff.

-- Учебный комментарий: Используем TEMP TABLE для подготовки дельты.
-- Это избавляет от дублирования логики hashdiff в UPDATE и INSERT блоках.
CREATE TEMP TABLE tmp_routes_src ON COMMIT DROP AS
SELECT
    route_no,
    departure_airport,
    arrival_airport,
    airplane_code,
    days_of_week,
    departure_time,
    duration,
    md5(
        COALESCE(departure_airport, '') || '|' ||
        COALESCE(arrival_airport, '') || '|' ||
        COALESCE(airplane_code, '') || '|' ||
        COALESCE(days_of_week::TEXT, '') || '|' ||
        COALESCE(departure_time::TEXT, '') || '|' ||
        COALESCE(duration::TEXT, '')
    ) AS hashdiff,
    ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
FROM ods.routes;

-- Statement 1: Закрыть устаревшие версии (valid_to = текущая дата).
UPDATE dds.dim_routes AS d
SET valid_to   = CURRENT_DATE,
    updated_at = now(),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
FROM tmp_routes_src AS s
WHERE s.rn = 1
    AND d.route_bk = s.route_no
    AND d.valid_to IS NULL
    AND d.hashdiff <> s.hashdiff;

-- Statement 1.1: Закрыть "исчезнувшие" маршруты.
UPDATE dds.dim_routes AS d
SET valid_to   = CURRENT_DATE,
    updated_at = now(),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
WHERE d.valid_to IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM tmp_routes_src AS s
        WHERE s.rn = 1
            AND s.route_no = d.route_bk
    );

-- Statement 2: Вставить новые версии (для изменённых и новых route_no).
WITH max_sk AS (
    -- Учебный комментарий: Генерация SK через MAX() + ROW_NUMBER()
    -- Этот подход работает безопасно только потому, что Airflow запускает
    -- джобы загрузки для одной таблицы строго последовательно (concurrency=1).
    SELECT COALESCE(MAX(route_sk), 0) AS v
    FROM dds.dim_routes
)
INSERT INTO dds.dim_routes (
    route_sk,
    route_bk,
    departure_airport,
    arrival_airport,
    airplane_code,
    departure_city,
    arrival_city,
    airplane_model,
    total_seats,
    days_of_week,
    departure_time,
    duration,
    hashdiff,
    valid_from,
    valid_to,
    created_at,
    updated_at,
    _load_id,
    _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.route_no)::INTEGER,
    s.route_no,
    s.departure_airport,
    s.arrival_airport,
    s.airplane_code,
    dep.city,
    arr.city,
    air.model,
    air.total_seats,
    s.days_of_week,
    s.departure_time,
    s.duration,
    s.hashdiff,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dds.dim_routes AS d2
            WHERE d2.route_bk = s.route_no
        ) THEN CURRENT_DATE
        ELSE '1900-01-01'::DATE
    END AS valid_from,
    NULL,
    now(),
    now(),
    '{{ run_id }}',
    now()
FROM tmp_routes_src AS s
LEFT JOIN dds.dim_airports AS dep ON s.departure_airport = dep.airport_bk
LEFT JOIN dds.dim_airports AS arr ON s.arrival_airport = arr.airport_bk
LEFT JOIN dds.dim_airplanes AS air ON s.airplane_code = air.airplane_bk
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM dds.dim_routes AS d
        WHERE d.route_bk = s.route_no
            AND d.valid_to IS NULL
            AND d.hashdiff = s.hashdiff
    );

-- Фаза 3: Обновление денормализованных атрибутов (refresh).
-- Нужна для SCD1-изменений в dim_airports/dim_airplanes (напр. переименование города).
-- Обновляем все версии (и текущие, и исторические), т.к. измерения SCD1.
-- При этом мы не перезаписываем _load_id и _load_ts, чтобы не размывать lineage версий.
UPDATE dds.dim_routes AS d
SET departure_city = dep.city,
    arrival_city   = arr.city,
    airplane_model = air.model,
    total_seats    = air.total_seats,
    updated_at     = now()
FROM dds.dim_airports AS dep,
     dds.dim_airports AS arr,
     dds.dim_airplanes AS air
WHERE dep.airport_bk = d.departure_airport
    AND arr.airport_bk = d.arrival_airport
    AND air.airplane_bk = d.airplane_code
    AND (
        d.departure_city IS DISTINCT FROM dep.city
        OR d.arrival_city IS DISTINCT FROM arr.city
        OR d.airplane_model IS DISTINCT FROM air.model
        OR d.total_seats IS DISTINCT FROM air.total_seats
    );

ANALYZE dds.dim_routes;

