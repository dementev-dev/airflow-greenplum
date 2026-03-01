-- Загрузка ODS по segments: SCD1 (UPDATE изменившихся + INSERT новых).
-- Используем паттерн Temporary Table для предотвращения гонки HWM между UPDATE и INSERT.

-- 1. Сбор дельты во временную таблицу.
CREATE TEMP TABLE tmp_segments_delta ON COMMIT DROP AS
WITH src AS (
    SELECT
        s.ticket_no,
        NULLIF(s.flight_id, '')::INTEGER   AS flight_id,
        s.fare_conditions,
        NULLIF(s.price, '')::NUMERIC(10,2) AS segment_amount,
        s.src_created_at_ts                 AS event_ts,
        s.batch_id,
        s.load_dttm,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no, s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.segments AS s
    -- Используем HWM (High Water Mark) по техническому времени STG
    WHERE s.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.segments)
)
SELECT * FROM src WHERE rn = 1;

-- 2. UPDATE существующих строк.
UPDATE ods.segments AS o
SET fare_conditions = s.fare_conditions,
    segment_amount  = s.segment_amount,
    event_ts        = s.event_ts,
    _load_id        = s.batch_id,    -- Сохраняем оригинальный lineage из STG
    _load_ts        = s.load_dttm    -- Фиксируем время STG как водяной знак для ODS
FROM tmp_segments_delta AS s
WHERE o.ticket_no = s.ticket_no
    AND o.flight_id = s.flight_id
    AND (
        o.fare_conditions IS DISTINCT FROM s.fare_conditions
        OR o.segment_amount IS DISTINCT FROM s.segment_amount
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- 3. INSERT новых строк.
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
    s.batch_id,
    s.load_dttm
FROM tmp_segments_delta AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.segments AS o
    WHERE o.ticket_no = s.ticket_no
        AND o.flight_id = s.flight_id
);

ANALYZE ods.segments;
