-- Загрузка ODS по segments: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.ticket_no,
        NULLIF(s.flight_id, '')::INTEGER   AS flight_id,
        s.fare_conditions,
        NULLIF(s.price, '')::NUMERIC(10,2) AS segment_amount,
        s.src_created_at_ts                 AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no, s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.segments AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.segments AS o
SET fare_conditions = s.fare_conditions,
    segment_amount  = s.segment_amount,
    event_ts        = s.event_ts,
    _load_id        = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts        = now()
FROM src AS s
WHERE s.rn = 1
    AND o.ticket_no = s.ticket_no
    AND o.flight_id = s.flight_id
    AND (
        o.fare_conditions IS DISTINCT FROM s.fare_conditions
        OR o.segment_amount IS DISTINCT FROM s.segment_amount
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.ticket_no,
        NULLIF(s.flight_id, '')::INTEGER   AS flight_id,
        s.fare_conditions,
        NULLIF(s.price, '')::NUMERIC(10,2) AS segment_amount,
        s.src_created_at_ts                 AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no, s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.segments AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.segments (
    ticket_no,
    flight_id,
    fare_conditions,
    segment_amount,
    event_ts,
    _load_id,
    _load_ts
)
SELECT
    s.ticket_no,
    s.flight_id,
    s.fare_conditions,
    s.segment_amount,
    s.event_ts,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.segments AS o
        WHERE o.ticket_no = s.ticket_no
            AND o.flight_id = s.flight_id
    );

ANALYZE ods.segments;
