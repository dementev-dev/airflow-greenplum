-- DQ для ODS airports.

DO $$
DECLARE
    v_batch_id TEXT := '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text;
    v_stg_batch_count BIGINT;
    v_dup_count BIGINT;
    v_missing_keys_count BIGINT;
    v_extra_keys_count BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Для snapshot-справочников пустой батч — ошибка.
    SELECT COUNT(*)
    INTO v_stg_batch_count
    FROM stg.airports
    WHERE batch_id = v_batch_id;

    IF v_stg_batch_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: batch_id=% для stg.airports пустой. Проверьте загрузку STG и PXF.',
            v_batch_id;
    END IF;

    -- В ODS не должно быть дублей по бизнес-ключу.
    SELECT COUNT(*) - COUNT(DISTINCT airport_code)
    INTO v_dup_count
    FROM ods.airports;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airports найдены дубликаты airport_code: %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT airport_code
        FROM stg.airports
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.airports AS o
        WHERE o.airport_code = s.airport_code
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airports отсутствуют ключи из stg.airports (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- В ODS не должно быть лишних ключей, которых нет в snapshot текущего батча.
    SELECT COUNT(*)
    INTO v_extra_keys_count
    FROM ods.airports AS o
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT DISTINCT airport_code
            FROM stg.airports
            WHERE batch_id = v_batch_id
        ) AS s
        WHERE s.airport_code = o.airport_code
    );

    IF v_extra_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airports найдены лишние ключи вне stg batch_id=%: %',
            v_batch_id,
            v_extra_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.airports
    WHERE airport_code IS NULL
        OR airport_code = ''
        OR airport_name IS NULL
        OR airport_name = ''
        OR city IS NULL
        OR city = ''
        OR country IS NULL
        OR country = ''
        OR timezone IS NULL
        OR timezone = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airports найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.airports ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
