-- Загрузка инкремента из stg.boarding_passes_ext в stg.boarding_passes.
-- Инкремент определяется по дате бронирования (book_date из bookings.bookings)
-- через JOIN с таблицами tickets и bookings (аналогично segments_load.sql).
--
-- Учебный комментарий: boarding_passes привязаны к билетам, а билеты — к бронированиям.
-- Чтобы инкремент boarding_passes совпадал с инкрементом tickets и segments,
-- используем ту же точку отсечения — book_date бронирования. Иначе при повторных
-- запусках full snapshot загрузит все boarding_passes, а tickets/segments — только
-- новые, и DQ обнаружит «сиротские» boarding_passes без соответствующих tickets.

-- CTE для определения максимальной даты загрузки предыдущего батча
WITH max_batch_ts AS (
    SELECT COALESCE(MAX(event_ts), TIMESTAMP '1900-01-01 00:00:00') AS max_ts
    FROM stg.boarding_passes
    WHERE _load_id <> '{{ run_id }}'::text
        OR _load_id IS NULL
)
INSERT INTO stg.boarding_passes (
    ticket_no,
    flight_id,
    seat_no,
    boarding_no,
    boarding_time,
    event_ts,
    _load_ts,
    _load_id
)
SELECT
    ext.ticket_no,
    ext.flight_id,
    ext.seat_no,
    ext.boarding_no::text,
    ext.boarding_time::text,
    b.book_date::timestamp,               -- временная метка из бронирования
    now(),
    '{{ run_id }}'::text
FROM stg.boarding_passes_ext AS ext
JOIN stg.tickets_ext AS t ON ext.ticket_no = t.ticket_no
JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
CROSS JOIN max_batch_ts AS mb
WHERE b.book_date > mb.max_ts
AND NOT EXISTS (
    -- Идемпотентность: при повторном запуске/ретрае не вставляем повторно те же строки.
    -- Считаем ключом строки (ticket_no, flight_id).
    SELECT 1
    FROM stg.boarding_passes AS bp
    WHERE bp._load_id = '{{ run_id }}'::text
        AND bp.ticket_no = ext.ticket_no
        AND bp.flight_id = ext.flight_id
);

-- Обновляем статистику для оптимизатора Greenplum
ANALYZE stg.boarding_passes;
