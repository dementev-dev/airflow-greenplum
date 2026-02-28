-- Загрузка ODS по airports: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
-- Нормализация JSON: извлекаем русские названия из полей с мультиязычностью.
-- Почему: источник хранит переводы как {"en": "...", "ru": "..."},
-- в ODS оставляем только один язык для упрощения downstream-логики.
WITH src AS (
    SELECT
        s.airport_code,
        s.airport_name::json->>'ru' AS airport_name,
        s.city::json->>'ru' AS city,
        s.country::json->>'ru' AS country,
        s.coordinates,
        s.timezone,
        ROW_NUMBER() OVER (
            PARTITION BY s.airport_code
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.airports AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.airports AS o
SET airport_name = s.airport_name,
    city         = s.city,
    country      = s.country,
    coordinates  = s.coordinates,
    timezone     = s.timezone,
    _load_id     = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts     = now()
FROM src AS s
WHERE s.rn = 1
    AND o.airport_code = s.airport_code
    AND (
        o.airport_name IS DISTINCT FROM s.airport_name
        OR o.city IS DISTINCT FROM s.city
        OR o.country IS DISTINCT FROM s.country
        OR o.coordinates IS DISTINCT FROM s.coordinates
        OR o.timezone IS DISTINCT FROM s.timezone
    );

-- Statement 2: INSERT новых строк.
-- Нормализация JSON: извлекаем русские названия из полей с мультиязычностью.
WITH src AS (
    SELECT
        s.airport_code,
        s.airport_name::json->>'ru' AS airport_name,
        s.city::json->>'ru' AS city,
        s.country::json->>'ru' AS country,
        s.coordinates,
        s.timezone,
        ROW_NUMBER() OVER (
            PARTITION BY s.airport_code
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.airports AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.airports (
    airport_code,
    airport_name,
    city,
    country,
    coordinates,
    timezone,
    _load_id,
    _load_ts
)
SELECT
    s.airport_code,
    s.airport_name,
    s.city,
    s.country,
    s.coordinates,
    s.timezone,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.airports AS o
        WHERE o.airport_code = s.airport_code
    );

-- Statement 3: DELETE ключей, которых нет в snapshot текущего батча.
-- Это делает ODS для справочника действительно "current state".
WITH src_keys AS (
    SELECT d.airport_code
    FROM (
        SELECT
            s.airport_code,
            ROW_NUMBER() OVER (
                PARTITION BY s.airport_code
                ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
            ) AS rn
        FROM stg.airports AS s
        WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
    ) AS d
    WHERE d.rn = 1
)
DELETE FROM ods.airports AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM src_keys AS s
    WHERE s.airport_code = o.airport_code
);

ANALYZE ods.airports;
