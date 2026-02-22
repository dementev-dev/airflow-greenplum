-- DQ для ODS boarding_passes.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_null_count BIGINT;
    v_orphan_segment_count BIGINT;
BEGIN
    -- Для этой таблицы пустой батч допустим.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.boarding_passes
    WHERE batch_id = v_batch_id;

    -- В ODS не должно быть дублей по составному бизнес-ключу.
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT ticket_no, flight_id
        FROM ods.boarding_passes
        GROUP BY ticket_no, flight_id
        HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.boarding_passes найдены дубликаты (ticket_no, flight_id): %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT ticket_no, NULLIF(flight_id, '')::INTEGER AS flight_id
        FROM stg.boarding_passes
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.boarding_passes AS o
        WHERE o.ticket_no = s.ticket_no
            AND o.flight_id = s.flight_id
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.boarding_passes отсутствуют ключи из stg.boarding_passes (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.boarding_passes
    WHERE ticket_no IS NULL
        OR ticket_no = ''
        OR flight_id IS NULL
        OR seat_no IS NULL
        OR seat_no = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.boarding_passes найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Ссылочная целостность: boarding_passes (ticket_no, flight_id) -> segments.
    SELECT COUNT(*)
    INTO v_orphan_segment_count
    FROM ods.boarding_passes AS bp
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.segments AS s
        WHERE s.ticket_no = bp.ticket_no
            AND s.flight_id = bp.flight_id
    );

    IF v_orphan_segment_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.boarding_passes найдены строки без соответствующего ods.segments: %',
            v_orphan_segment_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.boarding_passes ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
