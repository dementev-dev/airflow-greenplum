-- DQ для DDS dim_airplanes.

DO $$
DECLARE
    v_row_count BIGINT;
    v_dup_sk BIGINT;
    v_dup_bk BIGINT;
    v_missing_bk BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Таблица не пуста.
    SELECT COUNT(*)
    INTO v_row_count
    FROM dds.dim_airplanes;

    IF v_row_count = 0 THEN
        RAISE EXCEPTION 'DQ FAILED: dds.dim_airplanes пуста.';
    END IF;

    -- Нет дублей по SK.
    SELECT COUNT(*) - COUNT(DISTINCT airplane_sk)
    INTO v_dup_sk
    FROM dds.dim_airplanes;

    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airplanes найдены дубликаты airplane_sk: %',
            v_dup_sk;
    END IF;

    -- Нет дублей по BK.
    SELECT COUNT(*) - COUNT(DISTINCT airplane_bk)
    INTO v_dup_bk
    FROM dds.dim_airplanes;

    IF v_dup_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airplanes найдены дубликаты airplane_bk: %',
            v_dup_bk;
    END IF;

    -- Покрытие ODS: все airplane_code из ODS есть в DDS.
    SELECT COUNT(*)
    INTO v_missing_bk
    FROM (SELECT DISTINCT airplane_code FROM ods.airplanes) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dds.dim_airplanes AS d
        WHERE d.airplane_bk = s.airplane_code
    );

    IF v_missing_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airplanes отсутствуют ключи из ods.airplanes: %',
            v_missing_bk;
    END IF;

    -- Обязательные поля.
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_airplanes
    WHERE airplane_sk IS NULL
        OR airplane_bk IS NULL
        OR airplane_bk = ''
        OR model IS NULL
        OR model = ''
        OR total_seats IS NULL
        OR created_at IS NULL
        OR updated_at IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airplanes найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.dim_airplanes ок, строк=%',
        v_row_count;
END $$;
