-- Загрузка ODS по bookings: SCD1 (UPDATE изменившихся + INSERT новых).
-- Используем паттерн Temporary Table для предотвращения гонки HWM между UPDATE и INSERT.

-- 1. Сбор дельты во временную таблицу.
CREATE TEMP TABLE tmp_bookings_delta ON COMMIT DROP AS
WITH src AS (
    SELECT
        s.book_ref,
        NULLIF(s.book_date, '')::TIMESTAMP WITH TIME ZONE AS book_date,
        NULLIF(s.total_amount, '')::NUMERIC(10,2)         AS total_amount,
        s.src_created_at_ts                                AS event_ts,
        s.batch_id,
        s.load_dttm,
        ROW_NUMBER() OVER (
            PARTITION BY s.book_ref
            ORDER BY s.src_created_at_ts DESC NULLS LAST, s.load_dttm DESC
        ) AS rn
    FROM stg.bookings AS s
    -- Используем HWM (High Water Mark) по техническому времени STG
    WHERE s.load_dttm > (SELECT COALESCE(MAX(_load_ts), '1900-01-01 00:00:00'::TIMESTAMP) FROM ods.bookings)
)
SELECT * FROM src WHERE rn = 1;

-- 2. UPDATE существующих строк.
UPDATE ods.bookings AS o
SET book_date    = s.book_date,
    total_amount = s.total_amount,
    event_ts     = s.event_ts,
    _load_id     = s.batch_id,    -- Сохраняем оригинальный lineage из STG
    _load_ts     = s.load_dttm    -- Фиксируем время STG как водяной знак для ODS
FROM tmp_bookings_delta AS s
WHERE o.book_ref = s.book_ref
    AND (
        o.book_date IS DISTINCT FROM s.book_date
        OR o.total_amount IS DISTINCT FROM s.total_amount
        OR o.event_ts IS DISTINCT FROM s.event_ts
    );

-- 3. INSERT новых строк.
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
    s.batch_id,
    s.load_dttm
FROM tmp_bookings_delta AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM ods.bookings AS o
    WHERE o.book_ref = s.book_ref
);

ANALYZE ods.bookings;
