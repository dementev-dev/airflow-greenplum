-- Загрузка ODS по tickets: SCD1 (UPDATE изменившихся + INSERT новых).
-- Используем паттерн Temporary Table для предотвращения гонки HWM между UPDATE и INSERT.

-- 1. Сбор дельты во временную таблицу.
CREATE TEMP TABLE tmp_tickets_delta ON COMMIT DROP AS
WITH src AS (
    SELECT
        s.ticket_no,
        s.book_ref,
        s.passenger_id,
        s.passenger_name,
        NULLIF(s.outbound, '')::BOOLEAN AS is_outbound,
        s.src_created_at_ts             AS event_ts,
        s.batch_id,
        s.load_dttm,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_no
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.tickets AS s
    -- Используем HWM (High Water Mark) по техническому времени STG
    WHERE s.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.tickets)
)
SELECT * FROM src WHERE rn = 1;

-- 2. UPDATE существующих строк.
UPDATE ods.tickets AS o
SET book_ref       = s.book_ref,
    passenger_id   = s.passenger_id,
    passenger_name = s.passenger_name,
    is_outbound    = s.is_outbound,
    event_ts       = s.event_ts,
    _load_id       = s.batch_id,    -- Сохраняем оригинальный lineage из STG
    _load_ts       = s.load_dttm    -- Фиксируем время STG как водяной знак для ODS
FROM tmp_tickets_delta AS s
WHERE o.ticket_no = s.ticket_no
    AND (
        o.book_ref IS DISTINCT FROM s.book_ref
        OR o.passenger_id IS DISTINCT FROM s.passenger_id
        OR o.passenger_name IS DISTINCT FROM s.passenger_name
        OR o.is_outbound IS DISTINCT FROM s.is_outbound
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- 3. INSERT новых строк.
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
    s.batch_id,
    s.load_dttm
FROM tmp_tickets_delta AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.tickets AS o
    WHERE o.ticket_no = s.ticket_no
);

ANALYZE ods.tickets;
