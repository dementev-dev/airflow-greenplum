-- DQ для DDS dim_airports.

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
    FROM dds.dim_airports;

    IF v_row_count = 0 THEN
        RAISE EXCEPTION 'DQ FAILED: dds.dim_airports пуста.';
    END IF;

    -- Нет дублей по SK.
    SELECT COUNT(*) - COUNT(DISTINCT airport_sk)
    INTO v_dup_sk
    FROM dds.dim_airports;

    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airports найдены дубликаты airport_sk: %',
            v_dup_sk;
    END IF;

    -- Нет дублей по BK.
    SELECT COUNT(*) - COUNT(DISTINCT airport_bk)
    INTO v_dup_bk
    FROM dds.dim_airports;

    IF v_dup_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airports найдены дубликаты airport_bk: %',
            v_dup_bk;
    END IF;

    -- Покрытие ODS: все airport_code из ODS есть в DDS.
    SELECT COUNT(*)
    INTO v_missing_bk
    FROM (SELECT DISTINCT airport_code FROM ods.airports) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dds.dim_airports AS d
        WHERE d.airport_bk = s.airport_code
    );

    IF v_missing_bk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airports отсутствуют ключи из ods.airports: %',
            v_missing_bk;
    END IF;

    -- Обязательные поля.
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_airports
    WHERE airport_sk IS NULL
        OR airport_bk IS NULL
        OR airport_bk = ''
        OR airport_name IS NULL
        OR airport_name = ''
        OR city IS NULL
        OR city = ''
        OR country IS NULL
        OR country = ''
        OR timezone IS NULL
        OR timezone = ''
        OR created_at IS NULL
        OR updated_at IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_airports найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.dim_airports ок, строк=%',
        v_row_count;
END $$;
