-- DQ для DDS dim_tariffs.

DO $$
DECLARE
    v_row_count BIGINT;
    v_src_count BIGINT;
    v_dup_sk BIGINT;
    v_dup_bk BIGINT;
    v_missing_bk BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Для инкрементальных периодов без новых сегментов допускаем пустую dim_tariffs.
    SELECT COUNT(*)
    INTO v_row_count
    FROM dds.dim_tariffs;

    SELECT COUNT(DISTINCT fare_conditions)
    INTO v_src_count
    FROM ods.segments
    WHERE fare_conditions IS NOT NULL
        AND fare_conditions <> '';

    IF v_row_count = 0 AND v_src_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.dim_tariffs пуста при непустом источнике ods.segments (fare_conditions=%).',
            v_src_count;
    ELSIF v_row_count = 0 AND v_src_count = 0 THEN
        RAISE NOTICE
            'DQ PASSED: dds.dim_tariffs пуста, т.к. в ods.segments нет тарифов для загрузки.';
        RETURN;
    END IF;

    -- Нет дублей по SK.
    SELECT COUNT(*) - COUNT(DISTINCT tariff_sk)
    INTO v_dup_sk
    FROM dds.dim_tariffs;

    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_tariffs найдены дубликаты tariff_sk: %',
            v_dup_sk;
    END IF;

    -- Нет дублей по BK.
    SELECT COUNT(*) - COUNT(DISTINCT fare_conditions)
    INTO v_dup_bk
    FROM dds.dim_tariffs;

    IF v_dup_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_tariffs найдены дубликаты fare_conditions: %',
            v_dup_bk;
    END IF;

    -- Покрытие ODS: все fare_conditions из ods.segments есть в DDS.
    SELECT COUNT(*)
    INTO v_missing_bk
    FROM (
        SELECT DISTINCT fare_conditions
        FROM ods.segments
        WHERE fare_conditions IS NOT NULL
            AND fare_conditions <> ''
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dds.dim_tariffs AS d
        WHERE d.fare_conditions = s.fare_conditions
    );

    IF v_missing_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_tariffs отсутствуют значения fare_conditions из ods.segments: %',
            v_missing_bk;
    END IF;

    -- Обязательные поля.
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_tariffs
    WHERE tariff_sk IS NULL
        OR fare_conditions IS NULL
        OR fare_conditions = ''
        OR created_at IS NULL
        OR updated_at IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_tariffs найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.dim_tariffs ок, строк=%',
        v_row_count;
END $$;
