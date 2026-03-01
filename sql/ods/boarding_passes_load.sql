-- Загрузка ODS по boarding_passes: SCD1 (UPDATE изменившихся + INSERT новых).
-- Используем паттерн Temporary Table для предотвращения гонки HWM между UPDATE и INSERT.

-- 1. Сбор дельты во временную таблицу.
CREATE TEMP TABLE tmp_boarding_passes_delta ON COMMIT DROP AS
WITH src AS (
    SELECT
        s.ticket_no,
        NULLIF(s.flight_id, '')::INTEGER                           AS flight_id,
        s.seat_no,
        NULLIF(s.boarding_no, '')::INTEGER                         AS boarding_no,
        NULLIF(s.boarding_time, '')::TIMESTAMP WITH TIME ZONE      AS boarding_time,
        s.src_created_at_ts                                         AS event_ts,
        s.batch_id,
        s.load_dttm,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no, s.flight_id
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.boarding_passes AS s
    -- Используем HWM (High Water Mark) по техническому времени STG
    WHERE s.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.boarding_passes)
)
SELECT * FROM src WHERE rn = 1;

-- 2. UPDATE существующих строк.
UPDATE ods.boarding_passes AS o
SET seat_no       = s.seat_no,
    boarding_no   = s.boarding_no,
    boarding_time = s.boarding_time,
    event_ts      = s.event_ts,
    _load_id      = s.batch_id,    -- Сохраняем оригинальный lineage из STG
    _load_ts      = s.load_dttm    -- Фиксируем время STG как водяной знак для ODS
FROM tmp_boarding_passes_delta AS s
WHERE o.ticket_no = s.ticket_no
    AND o.flight_id = s.flight_id
    AND (
        o.seat_no IS DISTINCT FROM s.seat_no
        OR o.boarding_no IS DISTINCT FROM s.boarding_no
        OR o.boarding_time IS DISTINCT FROM s.boarding_time
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- 3. INSERT новых строк.
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
    s.batch_id,
    s.load_dttm
FROM tmp_boarding_passes_delta AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.boarding_passes AS o
    WHERE o.ticket_no = s.ticket_no
        AND o.flight_id = s.flight_id
);

ANALYZE ods.boarding_passes;
