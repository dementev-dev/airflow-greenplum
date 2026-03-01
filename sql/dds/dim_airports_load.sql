-- Загрузка DDS dim_airports: SCD1 UPSERT (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих записей (если атрибуты изменились).
UPDATE dds.dim_airports AS d
SET airport_name = s.airport_name,
    city         = s.city,
    country      = s.country,
    timezone     = s.timezone,
    coordinates  = s.coordinates,
    updated_at   = now(),
    _load_id     = '{{ run_id }}',
    _load_ts     = now()
FROM ods.airports AS s
WHERE d.airport_bk = s.airport_code
    AND (
        d.airport_name IS DISTINCT FROM s.airport_name
        OR d.city IS DISTINCT FROM s.city
        OR d.country IS DISTINCT FROM s.country
        OR d.timezone IS DISTINCT FROM s.timezone
        OR d.coordinates IS DISTINCT FROM s.coordinates
    );

-- Statement 2: INSERT новых записей (MAX(sk) + ROW_NUMBER()).
-- Учебный комментарий: Генерация SK через MAX() + ROW_NUMBER()
-- Этот подход работает безопасно только потому, что Airflow запускает
-- джобы загрузки для одной таблицы строго последовательно (concurrency=1).
-- При параллельной загрузке возникнет состояние гонки (race condition) и возможны дубли SK.
WITH max_sk AS (
    SELECT COALESCE(MAX(airport_sk), 0) AS v
    FROM dds.dim_airports
)
INSERT INTO dds.dim_airports (
    airport_sk,
    airport_bk,
    airport_name,
    city,
    country,
    timezone,
    coordinates,
    created_at,
    updated_at,
    _load_id,
    _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.airport_code)::INTEGER,
    s.airport_code,
    s.airport_name,
    s.city,
    s.country,
    s.timezone,
    s.coordinates,
    now(),
    now(),
    '{{ run_id }}',
    now()
FROM ods.airports AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.dim_airports AS d
    WHERE d.airport_bk = s.airport_code
);

ANALYZE dds.dim_airports;
