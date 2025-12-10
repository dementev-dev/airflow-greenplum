-- Проверка количества строк между источником stg.bookings_ext и стейджем stg.bookings.
-- Считаем строки за то же окно инкремента, что и при загрузке,
-- используя batch_id и "старый" максимум src_created_at_ts.

DO $$
DECLARE
    v_load_date   date    := {{ params.load_date }}::date;
    v_batch_id    text    := {{ params.batch_id | tojson }}::text;
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

    IF v_prev_ts IS NULL THEN
        -- Полная загрузка: считаем все строки во внешней таблице
        SELECT COUNT(*) INTO v_src_count FROM stg.bookings_ext;
    ELSE
        -- Инкремент: считаем только строки за текущее окно
        SELECT COUNT(*)
        INTO v_src_count
        FROM stg.bookings_ext
        WHERE book_date > v_prev_ts
          AND book_date <= (v_load_date + INTERVAL '1 day');
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

