-- Проверка количества строк между источником stg.bookings_ext и стейджем stg.bookings.
-- Считаем строки за то же окно инкремента, что и при загрузке:
-- все записи во внешней таблице с book_date больше максимального src_created_at_ts
-- из предыдущих батчей должны совпасть по количеству со строками текущего batch_id.

DO $$
DECLARE
    v_batch_id    text    := '{{ run_id }}'::text;
    v_prev_ts     timestamp;
    v_src_count   bigint;
    v_stg_count   bigint;
BEGIN
    -- Опорная метка: максимум src_created_at_ts среди предыдущих батчей
    SELECT max(src_created_at_ts)
    INTO v_prev_ts
    FROM stg.bookings
    WHERE batch_id <> v_batch_id
        OR batch_id IS NULL;

    -- Источник: считаем строки во внешней таблице, которые вошли в новое окно
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.bookings_ext
    WHERE book_date > COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');

    IF v_src_count = 0 THEN
        RAISE EXCEPTION
            'В источнике bookings_ext нет строк для окна инкремента (book_date > %). Проверьте генерацию данных (make bookings-init / make bookings-generate-day или таск generate_bookings_day).',
            COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');
    END IF;

    -- Считаем строки, реально вставленные в stg.bookings в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.bookings
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'Несовпадение количества строк при загрузке bookings: источник=%, stg=%. Проверьте окно инкремента и логи задач загрузки.',
            v_src_count,
            v_stg_count;
    END IF;

    RAISE NOTICE
        'Проверка количества строк пройдена: источник=%, stg=%',
        v_src_count,
        v_stg_count;
END $$;
