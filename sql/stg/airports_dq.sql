-- Проверки качества данных для airports (справочник)

DO $$
DECLARE
    v_batch_id TEXT := '{{ run_id }}'::text;
    v_src_count BIGINT;
    v_stg_count BIGINT;
    v_dup_count BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Источник: считаем все строки во внешней таблице
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.airports_ext;

    IF v_src_count = 0 THEN
        RAISE EXCEPTION
            'В источнике airports_ext нет строк.';
    END IF;

    -- Считаем строки, реально вставленные в stg.airports в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.airports
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты airport_code
    SELECT COUNT(*) - COUNT(DISTINCT airport_code)
    INTO v_dup_count
    FROM stg.airports AS a
    WHERE a.batch_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты airport_code (batch_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей: airport_code, airport_name, city, timezone
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.airports AS a
    WHERE a.batch_id = v_batch_id
        AND (a.airport_code IS NULL OR a.airport_code = ''
            OR a.airport_name IS NULL OR a.airport_name = ''
            OR a.city IS NULL OR a.city = ''
            OR a.timezone IS NULL OR a.timezone = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (airport_code, airport_name, city, timezone) (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: airports ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;
END $$;
