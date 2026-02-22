-- Загрузка ODS по tickets: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.ticket_no,
        s.book_ref,
        s.passenger_id,
        s.passenger_name,
        NULLIF(s.outbound, '')::BOOLEAN AS is_outbound,
        s.src_created_at_ts             AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.tickets AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.tickets AS o
SET book_ref       = s.book_ref,
    passenger_id   = s.passenger_id,
    passenger_name = s.passenger_name,
    is_outbound    = s.is_outbound,
    event_ts       = s.event_ts,
    _load_id       = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts       = now()
FROM src AS s
WHERE s.rn = 1
    AND o.ticket_no = s.ticket_no
    AND (
        o.book_ref IS DISTINCT FROM s.book_ref
        OR o.passenger_id IS DISTINCT FROM s.passenger_id
        OR o.passenger_name IS DISTINCT FROM s.passenger_name
        OR o.is_outbound IS DISTINCT FROM s.is_outbound
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.ticket_no,
        s.book_ref,
        s.passenger_id,
        s.passenger_name,
        NULLIF(s.outbound, '')::BOOLEAN AS is_outbound,
        s.src_created_at_ts             AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.tickets AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.tickets (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    is_outbound,
    event_ts,
    _load_id,
    _load_ts
)
SELECT
    s.ticket_no,
    s.book_ref,
    s.passenger_id,
    s.passenger_name,
    s.is_outbound,
    s.event_ts,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.tickets AS o
        WHERE o.ticket_no = s.ticket_no
    );

ANALYZE ods.tickets;
