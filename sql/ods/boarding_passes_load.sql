-- Загрузка ODS по boarding_passes: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.ticket_no,
        NULLIF(s.flight_id, '')::INTEGER                           AS flight_id,
        s.seat_no,
        NULLIF(s.boarding_no, '')::INTEGER                         AS boarding_no,
        NULLIF(s.boarding_time, '')::TIMESTAMP WITH TIME ZONE      AS boarding_time,
        s.src_created_at_ts                                         AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no, s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.boarding_passes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.boarding_passes AS o
SET seat_no       = s.seat_no,
    boarding_no   = s.boarding_no,
    boarding_time = s.boarding_time,
    event_ts      = s.event_ts,
    _load_id      = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts      = now()
FROM src AS s
WHERE s.rn = 1
    AND o.ticket_no = s.ticket_no
    AND o.flight_id = s.flight_id
    AND (
        o.seat_no IS DISTINCT FROM s.seat_no
        OR o.boarding_no IS DISTINCT FROM s.boarding_no
        OR o.boarding_time IS DISTINCT FROM s.boarding_time
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.ticket_no,
        NULLIF(s.flight_id, '')::INTEGER                           AS flight_id,
        s.seat_no,
        NULLIF(s.boarding_no, '')::INTEGER                         AS boarding_no,
        NULLIF(s.boarding_time, '')::TIMESTAMP WITH TIME ZONE      AS boarding_time,
        s.src_created_at_ts                                         AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no, s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.boarding_passes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.boarding_passes (
    ticket_no,
    flight_id,
    seat_no,
    boarding_no,
    boarding_time,
    event_ts,
    _load_id,
    _load_ts
)
SELECT
    s.ticket_no,
    s.flight_id,
    s.seat_no,
    s.boarding_no,
    s.boarding_time,
    s.event_ts,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.boarding_passes AS o
        WHERE o.ticket_no = s.ticket_no
            AND o.flight_id = s.flight_id
    );

ANALYZE ods.boarding_passes;
