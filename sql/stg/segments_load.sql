-- Загрузка инкремента из stg.segments_ext в stg.segments.
-- Инкремент определяется по дате бронирования (book_date из bookings.bookings)
-- через JOIN с таблицей tickets.

-- CTE для определения максимальной даты загрузки предыдущего батча
WITH max_batch_ts AS (
    SELECT COALESCE(MAX(src_created_at_ts), TIMESTAMP '1900-01-01 00:00:00') AS max_ts
    FROM stg.segments
    WHERE batch_id <> '{{ run_id }}'::text
        OR batch_id IS NULL
)
INSERT INTO stg.segments (
    ticket_no,
    flight_id,
    fare_conditions,
    price,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.ticket_no,
    ext.flight_id,
    ext.fare_conditions,
    ext.price::text,
    b.book_date::timestamp,
    now(),
    '{{ run_id }}'::text
FROM stg.segments_ext AS ext
JOIN stg.tickets_ext AS t ON ext.ticket_no = t.ticket_no
JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
CROSS JOIN max_batch_ts AS mb
WHERE b.book_date > mb.max_ts
AND NOT EXISTS (
    -- Защита от дублей в рамках одного batch_id
    SELECT 1
    FROM stg.segments AS s
    WHERE s.batch_id = '{{ run_id }}'::text
        AND s.ticket_no = ext.ticket_no
        AND s.flight_id = ext.flight_id
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.segments;
