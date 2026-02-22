-- DQ для ODS segments.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_null_count BIGINT;
    v_orphan_ticket_count BIGINT;
    v_orphan_flight_count BIGINT;
BEGIN
    -- Для инкрементальных таблиц пустой батч допустим.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.segments
    WHERE batch_id = v_batch_id;

    -- В ODS не должно быть дублей по составному бизнес-ключу.
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT ticket_no, flight_id
        FROM ods.segments
        GROUP BY ticket_no, flight_id
        HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.segments найдены дубликаты (ticket_no, flight_id): %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT ticket_no, NULLIF(flight_id, '')::INTEGER AS flight_id
        FROM stg.segments
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.segments AS o
        WHERE o.ticket_no = s.ticket_no
            AND o.flight_id = s.flight_id
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.segments отсутствуют ключи из stg.segments (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.segments
    WHERE ticket_no IS NULL
        OR ticket_no = ''
        OR flight_id IS NULL
        OR fare_conditions IS NULL
        OR fare_conditions = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.segments найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Ссылочная целостность: segments.ticket_no -> tickets.ticket_no.
    SELECT COUNT(*)
    INTO v_orphan_ticket_count
    FROM ods.segments AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.tickets AS t
        WHERE t.ticket_no = s.ticket_no
    );

    IF v_orphan_ticket_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.segments найдены строки без соответствующего tickets.ticket_no: %',
            v_orphan_ticket_count;
    END IF;

    -- Ссылочная целостность: segments.flight_id -> flights.flight_id.
    SELECT COUNT(*)
    INTO v_orphan_flight_count
    FROM ods.segments AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.flights AS f
        WHERE f.flight_id = s.flight_id
    );

    IF v_orphan_flight_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.segments найдены строки без соответствующего flights.flight_id: %',
            v_orphan_flight_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.segments ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
