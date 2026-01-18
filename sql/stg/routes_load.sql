-- Загрузка всех строк из stg.routes_ext в stg.routes (full load).
-- Используем batch_id для отслеживания загрузки.

INSERT INTO stg.routes (
    route_no,
    validity,
    departure_airport,
    arrival_airport,
    airplane_code,
    days_of_week,
    scheduled_time,
    duration,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.route_no::text,
    ext.validity::text,
    ext.departure_airport::text,
    ext.arrival_airport::text,
    ext.airplane_code::text,
    ext.days_of_week::text,
    ext.scheduled_time::text,
    ext.duration::text,
    now()::timestamp,
    now()::timestamp,
    '{{ run_id }}'::text
FROM stg.routes_ext AS ext
WHERE NOT EXISTS (
    -- Защита от дублей в рамках одного batch_id по составному ключу (route_no, validity)
    SELECT 1
    FROM stg.routes AS r
    WHERE r.batch_id = '{{ run_id }}'::text
        AND r.route_no = ext.route_no::text
        AND r.validity = ext.validity::text
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.routes;
