-- Загрузка ODS по bookings: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        s.book_ref,
        NULLIF(s.book_date, '')::TIMESTAMP WITH TIME ZONE AS book_date,
        NULLIF(s.total_amount, '')::NUMERIC(10,2)         AS total_amount,
        s.src_created_at_ts                                AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.book_ref
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.bookings AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.bookings AS o
SET book_date    = s.book_date,
    total_amount = s.total_amount,
    event_ts     = s.event_ts,
    _load_id     = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts     = now()
FROM src AS s
WHERE s.rn = 1
    AND o.book_ref = s.book_ref
    AND (
        o.book_date IS DISTINCT FROM s.book_date
        OR o.total_amount IS DISTINCT FROM s.total_amount
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        s.book_ref,
        NULLIF(s.book_date, '')::TIMESTAMP WITH TIME ZONE AS book_date,
        NULLIF(s.total_amount, '')::NUMERIC(10,2)         AS total_amount,
        s.src_created_at_ts                                AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.book_ref
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.bookings AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.bookings (
    book_ref,
    book_date,
    total_amount,
    event_ts,
    _load_id,
    _load_ts
)
SELECT
    s.book_ref,
    s.book_date,
    s.total_amount,
    s.event_ts,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.bookings AS o
        WHERE o.book_ref = s.book_ref
    );

ANALYZE ods.bookings;
