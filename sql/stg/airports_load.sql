-- Загрузка всех строк из stg.airports_ext в stg.airports (full load).
-- Используем batch_id для отслеживания загрузки.

INSERT INTO stg.airports (
    airport_code,
    airport_name,
    city,
    country,
    coordinates,
    timezone,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.airport_code::text,
    ext.airport_name::text,
    ext.city::text,
    ext.country::text,
    ext.coordinates::text,
    ext.timezone::text,
    now()::timestamp,
    now()::timestamp,
    '{{ run_id }}'::text
FROM stg.airports_ext AS ext
WHERE NOT EXISTS (
    -- Идемпотентность: при повторном запуске/ретрае не вставляем повторно те же строки в рамках текущего batch_id.
    SELECT 1
    FROM stg.airports AS a
    WHERE a.batch_id = '{{ run_id }}'::text
        AND a.airport_code = ext.airport_code::text
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.airports;
