-- DQ для ODS flights.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_null_count BIGINT;
    v_orphan_route_count BIGINT;
BEGIN
    -- Для инкрементальных таблиц пустой батч допустим.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.flights
    WHERE batch_id = v_batch_id;

    -- В ODS не должно быть дублей по бизнес-ключу.
    SELECT COUNT(*) - COUNT(DISTINCT flight_id)
    INTO v_dup_count
    FROM ods.flights;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.flights найдены дубликаты flight_id: %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT NULLIF(flight_id, '')::INTEGER AS flight_id
        FROM stg.flights
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.flights AS o
        WHERE o.flight_id = s.flight_id
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.flights отсутствуют ключи из stg.flights (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.flights
    WHERE flight_id IS NULL
        OR route_no IS NULL
        OR route_no = ''
        OR status IS NULL
        OR status = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.flights найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Ссылочная целостность: flights.route_no -> routes.route_no (упрощённо, без validity).
    SELECT COUNT(*)
    INTO v_orphan_route_count
    FROM ods.flights AS f
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.routes AS r
        WHERE r.route_no = f.route_no
    );

    IF v_orphan_route_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.flights найдены строки без соответствующего routes.route_no: %',
            v_orphan_route_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.flights ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
