-- Загрузка DDS dim_routes: SCD2 с hashdiff.

-- Statement 1: Закрыть устаревшие версии (valid_to = текущая дата).
WITH src AS (
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
            COALESCE(days_of_week, '') || '|' ||
            COALESCE(departure_time::TEXT, '') || '|' ||
            COALESCE(duration::TEXT, '')
        ) AS hashdiff,
        ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
    FROM ods.routes
)
UPDATE dds.dim_routes AS d
SET valid_to   = CURRENT_DATE,
    updated_at = now(),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
FROM src AS s
WHERE s.rn = 1
    AND d.route_bk = s.route_no
    AND d.valid_to IS NULL
    AND d.hashdiff <> s.hashdiff;

-- Statement 1.1: Закрыть "исчезнувшие" маршруты.
WITH src AS (
    SELECT
        route_no,
        ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
    FROM ods.routes
)
UPDATE dds.dim_routes AS d
SET valid_to   = CURRENT_DATE,
    updated_at = now(),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
WHERE d.valid_to IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM src AS s
        WHERE s.rn = 1
            AND s.route_no = d.route_bk
    );

-- Statement 2: Вставить новые версии (для изменённых и новых route_no).
WITH src AS (
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
            COALESCE(days_of_week, '') || '|' ||
            COALESCE(departure_time::TEXT, '') || '|' ||
            COALESCE(duration::TEXT, '')
        ) AS hashdiff,
        ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
    FROM ods.routes
),
max_sk AS (
    -- Учебный комментарий: Генерация SK через MAX() + ROW_NUMBER()
    -- Этот подход работает безопасно только потому, что Airflow запускает
    -- джобы загрузки для одной таблицы строго последовательно (concurrency=1).
    -- При параллельной загрузке возникнет состояние гонки (race condition) и возможны дубли SK.
    SELECT COALESCE(MAX(route_sk), 0) AS v
    FROM dds.dim_routes
)
INSERT INTO dds.dim_routes (
    route_sk,
    route_bk,
    departure_airport,
    arrival_airport,
    airplane_code,
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
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM dds.dim_routes AS d
        WHERE d.route_bk = s.route_no
            AND d.valid_to IS NULL
            AND d.hashdiff = s.hashdiff
    );

ANALYZE dds.dim_routes;
