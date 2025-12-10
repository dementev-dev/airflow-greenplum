-- Загрузка инкремента из stg.bookings_ext в stg.bookings.
-- Окно инкремента определяется по src_created_at_ts:
-- берем строки, где book_date больше максимального src_created_at_ts
-- среди "старых" батчей и не позже конца учебного дня.

INSERT INTO stg.bookings (
    book_ref,
    book_date,
    total_amount,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    book_ref::text,
    book_date::text,
    total_amount::text,
    book_date::timestamp,
    now(),
    {{ params.batch_id | tojson }}::text
FROM stg.bookings_ext
WHERE book_date > COALESCE(
    (
        SELECT max(src_created_at_ts)
        FROM stg.bookings
        WHERE batch_id <> {{ params.batch_id | tojson }}::text
            OR batch_id IS NULL
    ),
    TIMESTAMP '1900-01-01 00:00:00'
)
  AND book_date <= ({{ params.load_date }}::date + INTERVAL '1 day');

