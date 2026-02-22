-- DQ для ODS routes.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_extra_keys_count BIGINT;
    v_null_count BIGINT;
    v_orphan_departure_count BIGINT;
    v_orphan_arrival_count BIGINT;
    v_orphan_airplane_count BIGINT;
BEGIN
    -- Для snapshot-справочников пустой батч — ошибка.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.routes
    WHERE batch_id = v_batch_id;

    IF v_stg_batch_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: batch_id=% для stg.routes пустой. Проверьте загрузку STG и PXF.',
            v_batch_id;
    END IF;

    -- В ODS не должно быть дублей по составному бизнес-ключу.
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT route_no, validity
        FROM ods.routes
        GROUP BY route_no, validity
        HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes найдены дубликаты (route_no, validity): %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT route_no, validity
        FROM stg.routes
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.routes AS o
        WHERE o.route_no = s.route_no
            AND o.validity = s.validity
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes отсутствуют ключи из stg.routes (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- В ODS не должно быть лишних ключей, которых нет в snapshot текущего батча.
    SELECT COUNT(*)
    INTO v_extra_keys_count
    FROM ods.routes AS o
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT DISTINCT route_no, validity
            FROM stg.routes
            WHERE batch_id = v_batch_id
        ) AS s
        WHERE s.route_no = o.route_no
            AND s.validity = o.validity
    );

    IF v_extra_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes найдены лишние ключи вне stg batch_id=%: %',
            v_batch_id,
            v_extra_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.routes
    WHERE route_no IS NULL
        OR route_no = ''
        OR validity IS NULL
        OR validity = ''
        OR departure_airport IS NULL
        OR departure_airport = ''
        OR arrival_airport IS NULL
        OR arrival_airport = ''
        OR airplane_code IS NULL
        OR airplane_code = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Ссылочная целостность: departure_airport должен существовать в ods.airports.
    SELECT COUNT(*)
    INTO v_orphan_departure_count
    FROM ods.routes AS r
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.airports AS a
        WHERE a.airport_code = r.departure_airport
    );

    IF v_orphan_departure_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes найдены строки с невалидным departure_airport: %',
            v_orphan_departure_count;
    END IF;

    -- Ссылочная целостность: arrival_airport должен существовать в ods.airports.
    SELECT COUNT(*)
    INTO v_orphan_arrival_count
    FROM ods.routes AS r
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.airports AS a
        WHERE a.airport_code = r.arrival_airport
    );

    IF v_orphan_arrival_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes найдены строки с невалидным arrival_airport: %',
            v_orphan_arrival_count;
    END IF;

    -- Ссылочная целостность: airplane_code должен существовать в ods.airplanes.
    SELECT COUNT(*)
    INTO v_orphan_airplane_count
    FROM ods.routes AS r
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.airplanes AS a
        WHERE a.airplane_code = r.airplane_code
    );

    IF v_orphan_airplane_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.routes найдены строки с невалидным airplane_code: %',
            v_orphan_airplane_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.routes ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
