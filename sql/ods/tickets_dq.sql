-- DQ для ODS tickets.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_null_count BIGINT;
    v_orphan_booking_count BIGINT;
BEGIN
    -- Для инкрементальных таблиц пустой батч допустим.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.tickets
    WHERE batch_id = v_batch_id;

    -- В ODS не должно быть дублей по бизнес-ключу.
    SELECT COUNT(*) - COUNT(DISTINCT ticket_no)
    INTO v_dup_count
    FROM ods.tickets;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.tickets найдены дубликаты ticket_no: %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT ticket_no
        FROM stg.tickets
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.tickets AS o
        WHERE o.ticket_no = s.ticket_no
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.tickets отсутствуют ключи из stg.tickets (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.tickets
    WHERE ticket_no IS NULL
        OR ticket_no = ''
        OR book_ref IS NULL
        OR book_ref = ''
        OR passenger_id IS NULL
        OR passenger_id = ''
        OR passenger_name IS NULL
        OR passenger_name = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.tickets найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Ссылочная целостность: tickets.book_ref -> bookings.book_ref.
    SELECT COUNT(*)
    INTO v_orphan_booking_count
    FROM ods.tickets AS t
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.bookings AS b
        WHERE b.book_ref = t.book_ref
    );

    IF v_orphan_booking_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.tickets найдены строки без соответствующего bookings.book_ref: %',
            v_orphan_booking_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.tickets ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
