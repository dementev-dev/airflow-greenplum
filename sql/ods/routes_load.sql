-- Загрузка ODS по routes: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.route_no,
        s.validity,
        s.departure_airport,
        s.arrival_airport,
        s.airplane_code,
        s.days_of_week,
        NULLIF(s.scheduled_time, '')::TIME AS departure_time,
        NULLIF(s.duration, '')::INTERVAL   AS duration,
        ROW_NUMBER() OVER (
            PARTITION BY s.route_no, s.validity
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.routes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.routes AS o
SET departure_airport = s.departure_airport,
    arrival_airport   = s.arrival_airport,
    airplane_code     = s.airplane_code,
    days_of_week      = s.days_of_week,
    departure_time    = s.departure_time,
    duration          = s.duration,
    _load_id          = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts          = now()
FROM src AS s
WHERE s.rn = 1
    AND o.route_no = s.route_no
    AND o.validity = s.validity
    AND (
        o.departure_airport IS DISTINCT FROM s.departure_airport
        OR o.arrival_airport IS DISTINCT FROM s.arrival_airport
        OR o.airplane_code IS DISTINCT FROM s.airplane_code
        OR o.days_of_week IS DISTINCT FROM s.days_of_week
        OR o.departure_time IS DISTINCT FROM s.departure_time
        OR o.duration IS DISTINCT FROM s.duration
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.route_no,
        s.validity,
        s.departure_airport,
        s.arrival_airport,
        s.airplane_code,
        s.days_of_week,
        NULLIF(s.scheduled_time, '')::TIME AS departure_time,
        NULLIF(s.duration, '')::INTERVAL   AS duration,
        ROW_NUMBER() OVER (
            PARTITION BY s.route_no, s.validity
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.routes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.routes (
    route_no,
    validity,
    departure_airport,
    arrival_airport,
    airplane_code,
    days_of_week,
    departure_time,
    duration,
    _load_id,
    _load_ts
)
SELECT
    s.route_no,
    s.validity,
    s.departure_airport,
    s.arrival_airport,
    s.airplane_code,
    s.days_of_week,
    s.departure_time,
    s.duration,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.routes AS o
        WHERE o.route_no = s.route_no
            AND o.validity = s.validity
    );

-- Statement 3: DELETE ключей, которых нет в snapshot текущего батча.
-- Это делает ODS для справочника действительно "current state".
WITH src_keys AS (
    SELECT d.route_no, d.validity
    FROM (
        SELECT
            s.route_no,
            s.validity,
            ROW_NUMBER() OVER (
                PARTITION BY s.route_no, s.validity
                ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
            ) AS rn
        FROM stg.routes AS s
        WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
    ) AS d
    WHERE d.rn = 1
)
DELETE FROM ods.routes AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM src_keys AS s
    WHERE s.route_no = o.route_no
        AND s.validity = o.validity
);

ANALYZE ods.routes;
