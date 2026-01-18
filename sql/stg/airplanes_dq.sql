-- Проверки качества данных для airplanes (справочник)

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
    FROM stg.airplanes_ext;

    IF v_src_count = 0 THEN
        RAISE EXCEPTION
            'В источнике airplanes_ext нет строк. Проверьте: bookings-db запущен, PXF работает, STG DDL применён (bookings_stg_ddl или make ddl-gp).';
    END IF;

    -- Считаем строки, реально вставленные в stg.airplanes в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.airplanes
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты airplane_code
    SELECT COUNT(*) - COUNT(DISTINCT airplane_code)
    INTO v_dup_count
    FROM stg.airplanes AS a
    WHERE a.batch_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты airplane_code (batch_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей: airplane_code, model
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.airplanes AS a
    WHERE a.batch_id = v_batch_id
        AND (a.airplane_code IS NULL OR a.airplane_code = ''
            OR a.model IS NULL OR a.model = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (airplane_code, model) (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: airplanes ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;
END $$;
