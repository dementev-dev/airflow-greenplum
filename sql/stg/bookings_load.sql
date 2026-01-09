-- Загрузка инкремента из stg.bookings_ext в stg.bookings.
-- Окно инкремента определяется по src_created_at_ts:
-- берём строки, где book_date больше максимального src_created_at_ts
-- среди "старых" батчей; верхняя граница по дате не используется.

INSERT INTO stg.bookings (
    book_ref,
    book_date,
    total_amount,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.book_ref::text,
    ext.book_date::text,
    ext.total_amount::text,
    ext.book_date::timestamp,
    now(),
    '{{ run_id }}'::text
FROM stg.bookings_ext AS ext
WHERE ext.book_date > COALESCE(
    (
        SELECT max(src_created_at_ts)
        FROM stg.bookings
        WHERE batch_id <> '{{ run_id }}'::text
            OR batch_id IS NULL
    ),
    TIMESTAMP '1900-01-01 00:00:00'
)
    AND NOT EXISTS (
        SELECT 1
        FROM stg.bookings AS b
        WHERE b.batch_id = '{{ run_id }}'::text
            AND b.book_ref = ext.book_ref::text
    );
