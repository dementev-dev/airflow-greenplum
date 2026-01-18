-- Проверки качества данных для routes (справочник)

DO $$
DECLARE
    v_batch_id TEXT := '{{ run_id }}'::text;
    v_src_count BIGINT;
    v_stg_count BIGINT;
    v_dup_count BIGINT;
    v_null_count BIGINT;
    v_orphan_airports_count BIGINT;
    v_orphan_airplanes_count BIGINT;
BEGIN
    -- Источник: считаем все строки во внешней таблице
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.routes_ext;

    IF v_src_count = 0 THEN
        RAISE EXCEPTION
            'В источнике routes_ext нет строк.';
    END IF;

    -- Считаем строки, реально вставленные в stg.routes в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.routes
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты составного ключа (route_no, validity)
    -- Используем md5 от ROW, чтобы избежать коллизий при склейке строк.
    SELECT COUNT(*) - COUNT(DISTINCT md5(ROW(route_no, validity)::text))
    INTO v_dup_count
    FROM stg.routes AS r
    WHERE r.batch_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты (route_no, validity) (batch_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей: route_no, departure_airport, arrival_airport, airplane_code
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.routes AS r
    WHERE r.batch_id = v_batch_id
        AND (r.route_no IS NULL OR r.route_no = ''
            OR r.departure_airport IS NULL OR r.departure_airport = ''
            OR r.arrival_airport IS NULL OR r.arrival_airport = ''
            OR r.airplane_code IS NULL OR r.airplane_code = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (route_no, departure_airport, arrival_airport, airplane_code) (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    -- Проверка ссылочной целостности: departure_airport должен существовать в airports
    SELECT COUNT(*)
    INTO v_orphan_airports_count
    FROM stg.routes AS r
    LEFT JOIN stg.airports AS da
        ON r.departure_airport = da.airport_code
        AND da.batch_id = v_batch_id
    WHERE r.batch_id = v_batch_id
        AND da.airport_code IS NULL;

    IF v_orphan_airports_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены routes с несуществующим departure_airport в airports (batch_id=%): %',
            v_batch_id,
            v_orphan_airports_count;
    END IF;

    -- Проверка ссылочной целостности: arrival_airport должен существовать в airports
    SELECT COUNT(*)
    INTO v_orphan_airports_count
    FROM stg.routes AS r
    LEFT JOIN stg.airports AS aa
        ON r.arrival_airport = aa.airport_code
        AND aa.batch_id = v_batch_id
    WHERE r.batch_id = v_batch_id
        AND aa.airport_code IS NULL;

    IF v_orphan_airports_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены routes с несуществующим arrival_airport в airports (batch_id=%): %',
            v_batch_id,
            v_orphan_airports_count;
    END IF;

    -- Проверка ссылочной целостности: airplane_code должен существовать в airplanes
    SELECT COUNT(*)
    INTO v_orphan_airplanes_count
    FROM stg.routes AS r
    LEFT JOIN stg.airplanes AS a
        ON r.airplane_code = a.airplane_code
        AND a.batch_id = v_batch_id
    WHERE r.batch_id = v_batch_id
        AND a.airplane_code IS NULL;

    IF v_orphan_airplanes_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены routes с несуществующим airplane_code в airplanes (batch_id=%): %',
            v_batch_id,
            v_orphan_airplanes_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: routes ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;
END $$;
