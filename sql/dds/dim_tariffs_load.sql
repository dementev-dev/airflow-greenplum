-- Загрузка DDS dim_tariffs: SCD1 UPSERT (INSERT новых тарифов).

WITH src AS (
    SELECT DISTINCT
        s.fare_conditions
    FROM ods.segments AS s
    WHERE s.fare_conditions IS NOT NULL
        AND s.fare_conditions <> ''
),
-- Учебный комментарий: Генерация SK через MAX() + ROW_NUMBER()
-- Этот подход работает безопасно только потому, что Airflow запускает
-- джобы загрузки для одной таблицы строго последовательно (concurrency=1).
-- При параллельной загрузке возникнет состояние гонки (race condition) и возможны дубли SK.
max_sk AS (
    SELECT COALESCE(MAX(tariff_sk), 0) AS v
    FROM dds.dim_tariffs
)
INSERT INTO dds.dim_tariffs (
    tariff_sk,
    fare_conditions,
    created_at,
    updated_at,
    _load_id,
    _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.fare_conditions)::INTEGER,
    s.fare_conditions,
    now(),
    now(),
    '{{ run_id }}',
    now()
FROM src AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.dim_tariffs AS d
    WHERE d.fare_conditions = s.fare_conditions
);

ANALYZE dds.dim_tariffs;
