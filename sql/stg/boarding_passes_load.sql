-- Загрузка всех строк из stg.boarding_passes_ext в stg.boarding_passes.
-- Используем full snapshot: все строки при каждом запуске.
-- Используем batch_id для отслеживания загрузки.

INSERT INTO stg.boarding_passes (
    ticket_no,
    flight_id,
    seat_no,
    boarding_no,
    boarding_time,
    src_created_at_ts,
    load_dttm,
    batch_id
)
SELECT
    ext.ticket_no,
    ext.flight_id,
    ext.seat_no,
    ext.boarding_no::text,
    ext.boarding_time::text,
    now()::timestamp,
    now(),
    '{{ run_id }}'::text
FROM stg.boarding_passes_ext AS ext
WHERE NOT EXISTS (
    -- Идемпотентность: при повторном запуске/ретрае не вставляем повторно те же строки в рамках текущего batch_id.
    -- Считаем ключом строки (ticket_no, flight_id).
    SELECT 1
    FROM stg.boarding_passes AS bp
    WHERE bp.batch_id = '{{ run_id }}'::text
        AND bp.ticket_no = ext.ticket_no
        AND bp.flight_id = ext.flight_id
);

-- Обновляем статистику для оптимизатора Greenplum
-- Это критично для корректной работы оптимизатора и выбора оптимального плана выполнения
ANALYZE stg.boarding_passes;
