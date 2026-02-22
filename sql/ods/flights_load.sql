-- Загрузка ODS по flights: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH src AS (
    SELECT
        NULLIF(s.flight_id, '')::INTEGER                       AS flight_id,
        s.route_no,
        s.status,
        NULLIF(s.scheduled_departure, '')::TIMESTAMP WITH TIME ZONE AS scheduled_departure,
        NULLIF(s.scheduled_arrival, '')::TIMESTAMP WITH TIME ZONE   AS scheduled_arrival,
        NULLIF(s.actual_departure, '')::TIMESTAMP WITH TIME ZONE    AS actual_departure,
        NULLIF(s.actual_arrival, '')::TIMESTAMP WITH TIME ZONE      AS actual_arrival,
        s.src_created_at_ts                                           AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.flights AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
UPDATE ods.flights AS o
SET route_no            = s.route_no,
    status              = s.status,
    scheduled_departure = s.scheduled_departure,
    scheduled_arrival   = s.scheduled_arrival,
    actual_departure    = s.actual_departure,
    actual_arrival      = s.actual_arrival,
    event_ts            = s.event_ts,
    _load_id            = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    _load_ts            = now()
FROM src AS s
WHERE s.rn = 1
    AND o.flight_id = s.flight_id
    AND (
        o.route_no IS DISTINCT FROM s.route_no
        OR o.status IS DISTINCT FROM s.status
        OR o.scheduled_departure IS DISTINCT FROM s.scheduled_departure
        OR o.scheduled_arrival IS DISTINCT FROM s.scheduled_arrival
        OR o.actual_departure IS DISTINCT FROM s.actual_departure
        OR o.actual_arrival IS DISTINCT FROM s.actual_arrival
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- Statement 2: INSERT новых строк.
WITH src AS (
    SELECT
        NULLIF(s.flight_id, '')::INTEGER                       AS flight_id,
        s.route_no,
        s.status,
        NULLIF(s.scheduled_departure, '')::TIMESTAMP WITH TIME ZONE AS scheduled_departure,
        NULLIF(s.scheduled_arrival, '')::TIMESTAMP WITH TIME ZONE   AS scheduled_arrival,
        NULLIF(s.actual_departure, '')::TIMESTAMP WITH TIME ZONE    AS actual_departure,
        NULLIF(s.actual_arrival, '')::TIMESTAMP WITH TIME ZONE      AS actual_arrival,
        s.src_created_at_ts                                           AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.flights AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
INSERT INTO ods.flights (
    flight_id,
    route_no,
    status,
    scheduled_departure,
    scheduled_arrival,
    actual_departure,
    actual_arrival,
    event_ts,
    _load_id,
    _load_ts
)
SELECT
    s.flight_id,
    s.route_no,
    s.status,
    s.scheduled_departure,
    s.scheduled_arrival,
    s.actual_departure,
    s.actual_arrival,
    s.event_ts,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.flights AS o
        WHERE o.flight_id = s.flight_id
    );

ANALYZE ods.flights;
