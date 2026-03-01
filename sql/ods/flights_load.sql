-- Загрузка ODS по flights: SCD1 (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих строк.
WITH segment_flights AS (
    -- В segments текущего batch могут быть flight_id не только из stg.flights этого batch.
    -- Поэтому заранее собираем список flight_id из segments текущего batch.
    SELECT DISTINCT
        NULLIF(s.flight_id, '')::INTEGER AS flight_id
    FROM stg.segments AS s
    WHERE s.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.segments)
        AND s.flight_id IS NOT NULL
        AND s.flight_id <> ''
),
stg_flights_typed AS (
    SELECT
        NULLIF(s.flight_id, '')::INTEGER                            AS flight_id,
        s.route_no,
        s.status,
        NULLIF(s.scheduled_departure, '')::TIMESTAMP WITH TIME ZONE AS scheduled_departure,
        NULLIF(s.scheduled_arrival, '')::TIMESTAMP WITH TIME ZONE   AS scheduled_arrival,
        NULLIF(s.actual_departure, '')::TIMESTAMP WITH TIME ZONE    AS actual_departure,
        NULLIF(s.actual_arrival, '')::TIMESTAMP WITH TIME ZONE      AS actual_arrival,
        s.src_created_at_ts                                          AS event_ts,
        s.load_dttm,
        s.batch_id
    FROM stg.flights AS s
    WHERE s.flight_id IS NOT NULL
        AND s.flight_id <> ''
),
src_union AS (
    -- 1) Рейсы из текущего batch.
    SELECT
        f.flight_id,
        f.route_no,
        f.status,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.actual_departure,
        f.actual_arrival,
        f.event_ts,
        f.load_dttm
    FROM stg_flights_typed AS f
    WHERE f.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.flights)

    UNION ALL

    -- 2) Добираем из истории STG рейсы, на которые ссылаются segments текущего batch.
    SELECT
        f.flight_id,
        f.route_no,
        f.status,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.actual_departure,
        f.actual_arrival,
        f.event_ts,
        f.load_dttm
    FROM stg_flights_typed AS f
    JOIN segment_flights AS sf
        ON sf.flight_id = f.flight_id
),
src AS (
    SELECT
        u.flight_id,
        u.route_no,
        u.status,
        u.scheduled_departure,
        u.scheduled_arrival,
        u.actual_departure,
        u.actual_arrival,
        u.event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY u.flight_id
            ORDER BY u.event_ts DESC NULLS LAST, u.load_dttm DESC
        ) AS rn
    FROM src_union AS u
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
WITH segment_flights AS (
    SELECT DISTINCT
        NULLIF(s.flight_id, '')::INTEGER AS flight_id
    FROM stg.segments AS s
    WHERE s.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.segments)
        AND s.flight_id IS NOT NULL
        AND s.flight_id <> ''
),
stg_flights_typed AS (
    SELECT
        NULLIF(s.flight_id, '')::INTEGER                            AS flight_id,
        s.route_no,
        s.status,
        NULLIF(s.scheduled_departure, '')::TIMESTAMP WITH TIME ZONE AS scheduled_departure,
        NULLIF(s.scheduled_arrival, '')::TIMESTAMP WITH TIME ZONE   AS scheduled_arrival,
        NULLIF(s.actual_departure, '')::TIMESTAMP WITH TIME ZONE    AS actual_departure,
        NULLIF(s.actual_arrival, '')::TIMESTAMP WITH TIME ZONE      AS actual_arrival,
        s.src_created_at_ts                                          AS event_ts,
        s.load_dttm,
        s.batch_id
    FROM stg.flights AS s
    WHERE s.flight_id IS NOT NULL
        AND s.flight_id <> ''
),
src_union AS (
    SELECT
        f.flight_id,
        f.route_no,
        f.status,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.actual_departure,
        f.actual_arrival,
        f.event_ts,
        f.load_dttm
    FROM stg_flights_typed AS f
    WHERE f.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.flights)

    UNION ALL

    SELECT
        f.flight_id,
        f.route_no,
        f.status,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.actual_departure,
        f.actual_arrival,
        f.event_ts,
        f.load_dttm
    FROM stg_flights_typed AS f
    JOIN segment_flights AS sf
        ON sf.flight_id = f.flight_id
),
src AS (
    SELECT
        u.flight_id,
        u.route_no,
        u.status,
        u.scheduled_departure,
        u.scheduled_arrival,
        u.actual_departure,
        u.actual_arrival,
        u.event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY u.flight_id
            ORDER BY u.event_ts DESC NULLS LAST, u.load_dttm DESC
        ) AS rn
    FROM src_union AS u
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
    s.batch_id,
    now()
FROM src AS s
WHERE s.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM ods.flights AS o
        WHERE o.flight_id = s.flight_id
    );

ANALYZE ods.flights;
