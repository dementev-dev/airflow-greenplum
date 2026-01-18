-- Загрузка всех строк из stg.airplanes_ext в stg.airplanes (full load).
-- Используем batch_id для отслеживания загрузки.

INSERT INTO stg.airplanes (
    airplane_code,
    model,
    range,
    speed,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.airplane_code::text,
    ext.model::text,
    ext.range::text,
    ext.speed::text,
    now()::timestamp,
    now()::timestamp,
    '{{ run_id }}'::text
FROM stg.airplanes_ext AS ext
WHERE NOT EXISTS (
    -- Защита от дублей в рамках одного batch_id
    SELECT 1
    FROM stg.airplanes AS a
    WHERE a.batch_id = '{{ run_id }}'::text
        AND a.airplane_code = ext.airplane_code::text
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.airplanes;
