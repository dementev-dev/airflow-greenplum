-- DQ для ODS airplanes.

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
    FROM stg.airplanes
    WHERE batch_id = v_batch_id;

    IF v_stg_batch_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: batch_id=% для stg.airplanes пустой. Проверьте загрузку STG и PXF.',
            v_batch_id;
    END IF;

    -- В ODS не должно быть дублей по бизнес-ключу.
    SELECT COUNT(*) - COUNT(DISTINCT airplane_code)
    INTO v_dup_count
    FROM ods.airplanes;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airplanes найдены дубликаты airplane_code: %',
            v_dup_count;
    END IF;

    -- Все ключи из STG текущего батча должны присутствовать в ODS.
    SELECT COUNT(*)
    INTO v_missing_keys_count
    FROM (
        SELECT DISTINCT airplane_code
        FROM stg.airplanes
        WHERE batch_id = v_batch_id
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.airplanes AS o
        WHERE o.airplane_code = s.airplane_code
    );

    IF v_missing_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airplanes отсутствуют ключи из stg.airplanes (batch_id=%): %',
            v_batch_id,
            v_missing_keys_count;
    END IF;

    -- В ODS не должно быть лишних ключей, которых нет в snapshot текущего батча.
    SELECT COUNT(*)
    INTO v_extra_keys_count
    FROM ods.airplanes AS o
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT DISTINCT airplane_code
            FROM stg.airplanes
            WHERE batch_id = v_batch_id
        ) AS s
        WHERE s.airplane_code = o.airplane_code
    );

    IF v_extra_keys_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airplanes найдены лишние ключи вне stg batch_id=%: %',
            v_batch_id,
            v_extra_keys_count;
    END IF;

    -- Обязательные поля в ODS.
    SELECT COUNT(*)
    INTO v_null_count
    FROM ods.airplanes
    WHERE airplane_code IS NULL
        OR airplane_code = ''
        OR model IS NULL
        OR model = ''
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в ods.airplanes найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: ods.airplanes ок (batch_id=%): stg_batch_rows=%',
        v_batch_id,
        v_stg_batch_count;
END $$;
