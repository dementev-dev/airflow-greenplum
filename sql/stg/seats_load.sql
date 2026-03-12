-- Загрузка всех строк из stg.seats_ext в stg.seats (full load).
-- Используем _load_id для отслеживания загрузки.

INSERT INTO stg.seats (
    airplane_code,
    seat_no,
    fare_conditions,
    event_ts,
    _load_ts,
    _load_id
)
SELECT
    ext.airplane_code::text,
    ext.seat_no::text,
    ext.fare_conditions::text,
    now()::timestamp,
    now()::timestamp,
    '{{ run_id }}'::text
FROM stg.seats_ext AS ext
WHERE NOT EXISTS (
    -- Идемпотентность: при повторном запуске/ретрае не вставляем повторно те же строки в рамках текущего _load_id.
    -- Считаем ключом строки (airplane_code, seat_no).
    SELECT 1
    FROM stg.seats AS s
    WHERE s._load_id = '{{ run_id }}'::text
        AND s.airplane_code = ext.airplane_code::text
        AND s.seat_no = ext.seat_no::text
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.seats;
