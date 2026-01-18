-- Загрузка инкремента из stg.flights_ext в stg.flights.
-- Окно инкремента определяется по src_created_at_ts:
-- берём строки, где scheduled_departure больше максимального src_created_at_ts
-- среди "старых" батчей; верхняя граница по дате не используется.

-- CTE для определения максимальной даты загрузки предыдущего батча
WITH max_batch_ts AS (
    SELECT COALESCE(MAX(src_created_at_ts), TIMESTAMP '1900-01-01 00:00:00') AS max_ts
    FROM stg.flights
    WHERE batch_id <> '{{ run_id }}'::text
        OR batch_id IS NULL
)
INSERT INTO stg.flights (
    flight_id,
    route_no,
    status,
    scheduled_departure,
    scheduled_arrival,
    actual_departure,
    actual_arrival,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.flight_id::text,
    ext.route_no::text,
    ext.status::text,
    ext.scheduled_departure::text,
    ext.scheduled_arrival::text,
    ext.actual_departure::text,
    ext.actual_arrival::text,
    ext.scheduled_departure::timestamp,
    now(),
    '{{ run_id }}'::text
FROM stg.flights_ext AS ext
CROSS JOIN max_batch_ts AS mb
WHERE ext.scheduled_departure > mb.max_ts
AND NOT EXISTS (
    -- Идемпотентность: при повторном запуске/ретрае не вставляем повторно те же строки в рамках текущего batch_id.
    SELECT 1
    FROM stg.flights AS f
    WHERE f.batch_id = '{{ run_id }}'::text
        AND f.flight_id = ext.flight_id::text
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.flights;
