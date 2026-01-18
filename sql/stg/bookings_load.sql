-- Загрузка инкремента из stg.bookings_ext в stg.bookings.
-- Окно инкремента определяется по src_created_at_ts:
-- берём строки, где book_date больше максимального src_created_at_ts
-- среди "старых" батчей; верхняя граница по дате не используется.

-- CTE для определения максимальной даты загрузки предыдущего батча
WITH max_batch_ts AS (
    SELECT COALESCE(MAX(src_created_at_ts), TIMESTAMP '1900-01-01 00:00:00') AS max_ts
    FROM stg.bookings
    WHERE batch_id <> '{{ run_id }}'::text
        OR batch_id IS NULL
)
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
CROSS JOIN max_batch_ts AS mb
WHERE ext.book_date > mb.max_ts
AND NOT EXISTS (
    -- Идемпотентность: при повторном запуске/ретрае не вставляем повторно те же строки в рамках текущего batch_id.
    SELECT 1
    FROM stg.bookings AS b
    WHERE b.batch_id = '{{ run_id }}'::text
        AND b.book_ref = ext.book_ref::text
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.bookings;
