-- Загрузка инкремента из stg.tickets_ext в stg.tickets
-- Инкремент определяется по дате бронирования (book_date из bookings.bookings)

-- CTE для определения максимальной даты загрузки предыдущего батча
WITH max_batch_ts AS (
    SELECT COALESCE(MAX(src_created_at_ts), TIMESTAMP '1900-01-01 00:00:00') AS max_ts
    FROM stg.tickets
    WHERE batch_id <> '{{ run_id }}'::text
        OR batch_id IS NULL
)
INSERT INTO stg.tickets (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    outbound,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.ticket_no,
    ext.book_ref,
    ext.passenger_id,
    ext.passenger_name,
    ext.outbound,
    b.book_date::timestamp,               -- временная метка из бронирования
    now(),
    '{{ run_id }}'::text
FROM stg.tickets_ext AS ext
JOIN stg.bookings_ext AS b ON ext.book_ref = b.book_ref
CROSS JOIN max_batch_ts AS mb
WHERE b.book_date > mb.max_ts
AND NOT EXISTS (
    -- Идемпотентность: ticket_no — бизнес-ключ билета, не вставляем его повторно (включая ретраи/повторные запуски DAG).
    SELECT 1
    FROM stg.tickets AS t
    WHERE t.ticket_no = ext.ticket_no
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.tickets;
