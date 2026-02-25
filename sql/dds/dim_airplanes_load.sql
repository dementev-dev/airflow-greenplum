-- Загрузка DDS dim_airplanes: SCD1 UPSERT (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих записей (если атрибуты изменились).
WITH seats_agg AS (
    SELECT
        s.airplane_code,
        COUNT(*)::INTEGER AS total_seats
    FROM ods.seats AS s
    GROUP BY s.airplane_code
),
src AS (
    SELECT
        a.airplane_code,
        a.model,
        a.range_km,
        a.speed_kmh,
        COALESCE(sa.total_seats, 0) AS total_seats
    FROM ods.airplanes AS a
    LEFT JOIN seats_agg AS sa
        ON sa.airplane_code = a.airplane_code
)
UPDATE dds.dim_airplanes AS d
SET model       = s.model,
    range_km    = s.range_km,
    speed_kmh   = s.speed_kmh,
    total_seats = s.total_seats,
    updated_at  = now(),
    _load_id    = '{{ run_id }}',
    _load_ts    = now()
FROM src AS s
WHERE d.airplane_bk = s.airplane_code
    AND (
        d.model IS DISTINCT FROM s.model
        OR d.range_km IS DISTINCT FROM s.range_km
        OR d.speed_kmh IS DISTINCT FROM s.speed_kmh
        OR d.total_seats IS DISTINCT FROM s.total_seats
    );

-- Statement 2: INSERT новых записей (MAX(sk) + ROW_NUMBER()).
WITH seats_agg AS (
    SELECT
        s.airplane_code,
        COUNT(*)::INTEGER AS total_seats
    FROM ods.seats AS s
    GROUP BY s.airplane_code
),
src AS (
    SELECT
        a.airplane_code,
        a.model,
        a.range_km,
        a.speed_kmh,
        COALESCE(sa.total_seats, 0) AS total_seats
    FROM ods.airplanes AS a
    LEFT JOIN seats_agg AS sa
        ON sa.airplane_code = a.airplane_code
),
max_sk AS (
    SELECT COALESCE(MAX(airplane_sk), 0) AS v
    FROM dds.dim_airplanes
)
INSERT INTO dds.dim_airplanes (
    airplane_sk,
    airplane_bk,
    model,
    range_km,
    speed_kmh,
    total_seats,
    created_at,
    updated_at,
    _load_id,
    _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.airplane_code)::INTEGER,
    s.airplane_code,
    s.model,
    s.range_km,
    s.speed_kmh,
    s.total_seats,
    now(),
    now(),
    '{{ run_id }}',
    now()
FROM src AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.dim_airplanes AS d
    WHERE d.airplane_bk = s.airplane_code
);

ANALYZE dds.dim_airplanes;
