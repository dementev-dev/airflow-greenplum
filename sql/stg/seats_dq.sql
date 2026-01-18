-- Проверки качества данных для seats (справочник)

DO $$
DECLARE
    v_batch_id TEXT := '{{ run_id }}'::text;
    v_src_count BIGINT;
    v_stg_count BIGINT;
    v_dup_count BIGINT;
    v_null_count BIGINT;
    v_orphan_airplanes_count BIGINT;
BEGIN
    -- Источник: считаем все строки во внешней таблице
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.seats_ext;

    IF v_src_count = 0 THEN
        RAISE EXCEPTION
            'В источнике seats_ext нет строк.';
    END IF;

    -- Считаем строки, реально вставленные в stg.seats в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.seats
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты составного ключа (airplane_code, seat_no)
    SELECT COUNT(*) - COUNT(DISTINCT airplane_code || '|' || seat_no)
    INTO v_dup_count
    FROM stg.seats AS s
    WHERE s.batch_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты (airplane_code, seat_no) (batch_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей: airplane_code, seat_no, fare_conditions
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.seats AS s
    WHERE s.batch_id = v_batch_id
        AND (s.airplane_code IS NULL OR s.airplane_code = ''
            OR s.seat_no IS NULL OR s.seat_no = ''
            OR s.fare_conditions IS NULL OR s.fare_conditions = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (airplane_code, seat_no, fare_conditions) (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    -- Проверка ссылочной целостности: airplane_code должен существовать в airplanes
    SELECT COUNT(*)
    INTO v_orphan_airplanes_count
    FROM stg.seats AS s
    LEFT JOIN stg.airplanes AS a
        ON s.airplane_code = a.airplane_code
        AND a.batch_id = v_batch_id
    WHERE s.batch_id = v_batch_id
        AND a.airplane_code IS NULL;

    IF v_orphan_airplanes_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены seats с несуществующим airplane_code в airplanes (batch_id=%): %',
            v_batch_id,
            v_orphan_airplanes_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: seats ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;
END $$;
