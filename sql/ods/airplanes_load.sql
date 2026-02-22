-- Загрузка ODS по airplanes: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.airplane_code,
        s.model,
        NULLIF(s.range, '')::INTEGER AS range_km,
        NULLIF(s.speed, '')::INTEGER AS speed_kmh,
        ROW_NUMBER() OVER (
            PARTITION BY s.airplane_code
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.airplanes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.airplanes AS o
SET model        = s.model,
    range_km     = s.range_km,
    speed_kmh    = s.speed_kmh,
    _load_id     = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts     = now()
FROM src AS s
WHERE s.rn = 1
    AND o.airplane_code = s.airplane_code
    AND (
        o.model IS DISTINCT FROM s.model
        OR o.range_km IS DISTINCT FROM s.range_km
        OR o.speed_kmh IS DISTINCT FROM s.speed_kmh
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.airplane_code,
        s.model,
        NULLIF(s.range, '')::INTEGER AS range_km,
        NULLIF(s.speed, '')::INTEGER AS speed_kmh,
        ROW_NUMBER() OVER (
            PARTITION BY s.airplane_code
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.airplanes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.airplanes (
    airplane_code,
    model,
    range_km,
    speed_kmh,
    _load_id,
    _load_ts
)
SELECT
    s.airplane_code,
    s.model,
    s.range_km,
    s.speed_kmh,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.airplanes AS o
        WHERE o.airplane_code = s.airplane_code
    );

-- Statement 3: DELETE ключей, которых нет в snapshot текущего батча.
-- Это делает ODS для справочника действительно "current state".
WITH src_keys AS (
    SELECT d.airplane_code
    FROM (
        SELECT
            s.airplane_code,
            ROW_NUMBER() OVER (
                PARTITION BY s.airplane_code
                ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
            ) AS rn
        FROM stg.airplanes AS s
        WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
    ) AS d
    WHERE d.rn = 1
)
DELETE FROM ods.airplanes AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM src_keys AS s
    WHERE s.airplane_code = o.airplane_code
);

ANALYZE ods.airplanes;
