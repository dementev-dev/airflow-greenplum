-- DQ для ODS bookings.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Для инкрементальных таблиц пустой батч допустим.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.bookings
    WHERE batch_id = v_batch_id;

    -- В ODS не должно быть дублей по бизнес-ключу.
    SELECT COUNT(*) - COUNT(DISTINCT book_ref)
    INTO v_dup_count
    FROM ods.bookings;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.bookings найдены дубликаты book_ref: %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT book_ref
        FROM stg.bookings
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.bookings AS o
        WHERE o.book_ref = s.book_ref
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.bookings отсутствуют ключи из stg.bookings (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.bookings
    WHERE book_ref IS NULL
        OR book_ref = ''
        OR book_date IS NULL
        OR total_amount IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.bookings найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.bookings ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
