-- Загрузка инкремента из stg.tickets_ext в stg.tickets
-- Инкремент определяется по дате бронирования (book_date из bookings.bookings)

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
WHERE b.book_date > COALESCE(
    (
        SELECT max(src_created_at_ts)
        FROM stg.tickets
        WHERE batch_id <> '{{ run_id }}'::text
            OR batch_id IS NULL
    ),
    TIMESTAMP '1900-01-01 00:00:00'
)
AND NOT EXISTS (
    -- Защита от дублей: ticket_no в источнике уникален, и в stg его не дублируем.
    SELECT 1
    FROM stg.tickets AS t
    WHERE t.ticket_no = ext.ticket_no
);
