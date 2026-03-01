-- Загрузка ODS по flights: SCD1 (UPDATE изменившихся + INSERT новых).
-- Используем паттерн Temporary Table для предотвращения гонки HWM между UPDATE и INSERT.

-- 1. Сбор дельты во временную таблицу.
CREATE TEMP TABLE tmp_flights_delta ON COMMIT DROP AS
WITH segment_flights AS (
    -- В segments текущего batch могут быть flight_id не только из stg.flights этого batch.
    -- Поэтому заранее собираем список flight_id из segments текущего batch (по HWM).
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
    -- 1) Рейсы из текущего batch (по HWM).
    SELECT
        f.flight_id,
        f.route_no,
        f.status,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.actual_departure,
        f.actual_arrival,
        f.event_ts,
        f.load_dttm,
        f.batch_id
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
        f.load_dttm,
        f.batch_id
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
        u.batch_id,
        u.load_dttm,
        ROW_NUMBER() OVER (
            PARTITION BY u.flight_id
            ORDER BY u.event_ts DESC NULLS LAST, u.load_dttm DESC
        ) AS rn
    FROM src_union AS u
)
SELECT * FROM src WHERE rn = 1;

-- 2. UPDATE существующих строк.
UPDATE ods.flights AS o
SET route_no            = s.route_no,
    status              = s.status,
    scheduled_departure = s.scheduled_departure,
    scheduled_arrival   = s.scheduled_arrival,
    actual_departure    = s.actual_departure,
    actual_arrival      = s.actual_arrival,
    event_ts            = s.event_ts,
    _load_id            = s.batch_id,    -- Сохраняем оригинальный lineage из STG
    _load_ts            = s.load_dttm    -- Фиксируем время STG как водяной знак для ODS
FROM tmp_flights_delta AS s
WHERE o.flight_id = s.flight_id
    AND (
        o.route_no IS DISTINCT FROM s.route_no
        OR o.status IS DISTINCT FROM s.status
        OR o.scheduled_departure IS DISTINCT FROM s.scheduled_departure
        OR o.scheduled_arrival IS DISTINCT FROM s.scheduled_arrival
        OR o.actual_departure IS DISTINCT FROM s.actual_departure
        OR o.actual_arrival IS DISTINCT FROM s.actual_arrival
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- 3. INSERT новых строк.
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
    s.load_dttm
FROM tmp_flights_delta AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.flights AS o
    WHERE o.flight_id = s.flight_id
);

ANALYZE ods.flights;
