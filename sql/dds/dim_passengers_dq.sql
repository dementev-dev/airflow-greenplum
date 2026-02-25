-- DQ для DDS dim_passengers.

DO $$
DECLARE
    v_row_count BIGINT;
    v_src_count BIGINT;
    v_dup_sk BIGINT;
    v_dup_bk BIGINT;
    v_missing_bk BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Для инкрементальных периодов без новых билетов допускаем пустую dim_passengers.
    SELECT COUNT(*)
    INTO v_row_count
    FROM dds.dim_passengers;

    SELECT COUNT(DISTINCT passenger_id)
    INTO v_src_count
    FROM ods.tickets
    WHERE passenger_id IS NOT NULL
        AND passenger_id <> '';

    IF v_row_count = 0 AND v_src_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.dim_passengers пуста при непустом источнике ods.tickets (passenger_id=%).',
            v_src_count;
    ELSIF v_row_count = 0 AND v_src_count = 0 THEN
        RAISE NOTICE
            'DQ PASSED: dds.dim_passengers пуста, т.к. в ods.tickets нет passenger_id для загрузки.';
        RETURN;
    END IF;

    -- Нет дублей по SK.
    SELECT COUNT(*) - COUNT(DISTINCT passenger_sk)
    INTO v_dup_sk
    FROM dds.dim_passengers;

    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_passengers найдены дубликаты passenger_sk: %',
            v_dup_sk;
    END IF;

    -- Нет дублей по BK.
    SELECT COUNT(*) - COUNT(DISTINCT passenger_bk)
    INTO v_dup_bk
    FROM dds.dim_passengers;

    IF v_dup_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_passengers найдены дубликаты passenger_bk: %',
            v_dup_bk;
    END IF;

    -- Покрытие ODS: все passenger_id из ods.tickets есть в DDS.
    SELECT COUNT(*)
    INTO v_missing_bk
    FROM (
        SELECT DISTINCT passenger_id
        FROM ods.tickets
        WHERE passenger_id IS NOT NULL
            AND passenger_id <> ''
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dds.dim_passengers AS d
        WHERE d.passenger_bk = s.passenger_id
    );

    IF v_missing_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_passengers отсутствуют passenger_id из ods.tickets: %',
            v_missing_bk;
    END IF;

    -- Обязательные поля.
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_passengers
    WHERE passenger_sk IS NULL
        OR passenger_bk IS NULL
        OR passenger_bk = ''
        OR passenger_name IS NULL
        OR passenger_name = ''
        OR created_at IS NULL
        OR updated_at IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_passengers найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.dim_passengers ок, строк=%',
        v_row_count;
END $$;
