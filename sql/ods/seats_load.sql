-- Загрузка ODS по seats: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.airplane_code,
        s.seat_no,
        s.fare_conditions,
        ROW_NUMBER() OVER (
            PARTITION BY s.airplane_code, s.seat_no
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.seats AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.seats AS o
SET fare_conditions = s.fare_conditions,
    _load_id        = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts        = now()
FROM src AS s
WHERE s.rn = 1
    AND o.airplane_code = s.airplane_code
    AND o.seat_no = s.seat_no
    AND o.fare_conditions IS DISTINCT FROM s.fare_conditions;

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.airplane_code,
        s.seat_no,
        s.fare_conditions,
        ROW_NUMBER() OVER (
            PARTITION BY s.airplane_code, s.seat_no
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.seats AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.seats (
    airplane_code,
    seat_no,
    fare_conditions,
    _load_id,
    _load_ts
)
SELECT
    s.airplane_code,
    s.seat_no,
    s.fare_conditions,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.seats AS o
        WHERE o.airplane_code = s.airplane_code
            AND o.seat_no = s.seat_no
    );

-- Statement 3: DELETE ключей, которых нет в snapshot текущего батча.
-- Это делает ODS для справочника действительно "current state".
WITH src_keys AS (
    SELECT d.airplane_code, d.seat_no
    FROM (
        SELECT
            s.airplane_code,
            s.seat_no,
            ROW_NUMBER() OVER (
                PARTITION BY s.airplane_code, s.seat_no
                ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
            ) AS rn
        FROM stg.seats AS s
        WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
    ) AS d
    WHERE d.rn = 1
)
DELETE FROM ods.seats AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM src_keys AS s
    WHERE s.airplane_code = o.airplane_code
        AND s.seat_no = o.seat_no
);

ANALYZE ods.seats;
