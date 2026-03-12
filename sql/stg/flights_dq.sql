-- Проверки качества данных для flights

DO $$
DECLARE
    v_batch_id        TEXT      := '{{ run_id }}'::text;
    v_prev_ts         TIMESTAMP;
    v_src_count       BIGINT;
    v_stg_count       BIGINT;
    v_dup_count       BIGINT;
    v_null_count      BIGINT;
    v_orphan_route_count BIGINT;
BEGIN
    -- Опорная метка: максимум event_ts среди предыдущих батчей
    SELECT max(event_ts)
    INTO v_prev_ts
    FROM stg.flights
    WHERE _load_id <> v_batch_id
        OR _load_id IS NULL;

    -- Источник: считаем строки во внешней таблице, которые вошли в окно инкремента
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.flights_ext
    WHERE scheduled_departure > COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');

    IF v_src_count = 0 THEN
        -- Пустое окно инкремента допустимо: новых данных может не быть.
        -- В этом случае ожидаем, что в текущем _load_id тоже 0 строк.
        SELECT COUNT(*)
        INTO v_stg_count
        FROM stg.flights
        WHERE _load_id = v_batch_id;

        IF v_stg_count <> 0 THEN
            RAISE EXCEPTION
                'DQ FAILED: источник flights_ext за окно инкремента пустой, но в stg.flights есть строки текущего _load_id (_load_id=%): %',
                v_batch_id,
                v_stg_count;
        END IF;

        RAISE NOTICE
            'В источнике flights_ext нет строк для окна инкремента (scheduled_departure > %). Пропускаем DQ проверки (_load_id=%).',
            COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00'),
            v_batch_id;
        RETURN;
    END IF;

    -- Считаем строки, реально вставленные в stg.flights в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.flights
    WHERE _load_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты flight_id
    SELECT COUNT(*) - COUNT(DISTINCT flight_id)
    INTO v_dup_count
    FROM stg.flights AS f
    WHERE f._load_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты flight_id (_load_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей (flight_id, route_no, status, scheduled_departure)
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.flights AS f
    WHERE f._load_id = v_batch_id
        AND (f.flight_id IS NULL OR f.flight_id = ''
             OR f.route_no IS NULL OR f.route_no = ''
             OR f.status IS NULL OR f.status = ''
             OR f.scheduled_departure IS NULL OR f.scheduled_departure = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (_load_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    -- Проверка ссылочной целостности: все flights должны иметь соответствующие routes
    SELECT COUNT(*)
    INTO v_orphan_route_count
    FROM stg.flights AS f
    LEFT JOIN stg.routes AS r
        ON f.route_no = r.route_no
        AND r._load_id = v_batch_id
    WHERE f._load_id = v_batch_id
        AND r.route_no IS NULL;

    IF v_orphan_route_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены flights без соответствующих routes (_load_id=%): %',
            v_batch_id,
            v_orphan_route_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: flights ок (_load_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;
END $$;
