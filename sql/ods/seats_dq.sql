-- DQ для ODS seats.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_extra_keys_count BIGINT;
    v_null_count BIGINT;
    v_orphan_airplane_count BIGINT;
BEGIN
    -- Для snapshot-справочников пустой батч — ошибка.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.seats
    WHERE batch_id = v_batch_id;

    IF v_stg_batch_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: batch_id=% для stg.seats пустой. Проверьте загрузку STG и PXF.',
            v_batch_id;
    END IF;

    -- В ODS не должно быть дублей по составному бизнес-ключу.
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT airplane_code, seat_no
        FROM ods.seats
        GROUP BY airplane_code, seat_no
        HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.seats найдены дубликаты (airplane_code, seat_no): %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT airplane_code, seat_no
        FROM stg.seats
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.seats AS o
        WHERE o.airplane_code = s.airplane_code
            AND o.seat_no = s.seat_no
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.seats отсутствуют ключи из stg.seats (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- В ODS не должно быть лишних ключей, которых нет в snapshot текущего батча.
    SELECT COUNT(*)
    INTO v_extra_keys_count
    FROM ods.seats AS o
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT DISTINCT airplane_code, seat_no
            FROM stg.seats
            WHERE batch_id = v_batch_id
        ) AS s
        WHERE s.airplane_code = o.airplane_code
            AND s.seat_no = o.seat_no
    );

    IF v_extra_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.seats найдены лишние ключи вне stg batch_id=%: %',
            v_batch_id,
            v_extra_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.seats
    WHERE airplane_code IS NULL
        OR airplane_code = ''
        OR seat_no IS NULL
        OR seat_no = ''
        OR fare_conditions IS NULL
        OR fare_conditions = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.seats найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Ссылочная целостность: airplane_code должен существовать в ods.airplanes.
    SELECT COUNT(*)
    INTO v_orphan_airplane_count
    FROM ods.seats AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.airplanes AS a
        WHERE a.airplane_code = s.airplane_code
    );

    IF v_orphan_airplane_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.seats найдены строки с невалидным airplane_code: %',
            v_orphan_airplane_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.seats ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
