-- DQ для DDS dim_routes (SCD2).

DO $$
DECLARE
    v_row_count BIGINT;
    v_dup_sk BIGINT;
    v_dup_current BIGINT;
    v_overlap_count BIGINT;
    v_missing_count BIGINT;
    v_orphan_current BIGINT;
    v_null_count BIGINT;
BEGIN
    -- Таблица не пуста.
    SELECT COUNT(*)
    INTO v_row_count
    FROM dds.dim_routes;

    IF v_row_count = 0 THEN
        RAISE EXCEPTION 'DQ FAILED: dds.dim_routes пуста.';
    END IF;

    -- Нет дублей по SK.
    SELECT COUNT(*) - COUNT(DISTINCT route_sk)
    INTO v_dup_sk
    FROM dds.dim_routes;

    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены дубликаты route_sk: %',
            v_dup_sk;
    END IF;

    -- Корректность интервалов (valid_from <= valid_to для закрытых версий).
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_routes
    WHERE valid_to IS NOT NULL
        AND valid_from > valid_to;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены версии с valid_from > valid_to: %',
            v_null_count;
    END IF;

    -- Нет перекрытий интервалов для одного route_bk.
    SELECT COUNT(*)
    INTO v_overlap_count
    FROM (
        SELECT 1
        FROM dds.dim_routes AS d1
        JOIN dds.dim_routes AS d2
            ON d1.route_bk = d2.route_bk
            AND d1.route_sk < d2.route_sk
            AND d1.valid_from < COALESCE(d2.valid_to, DATE '9999-12-31')
            AND d2.valid_from < COALESCE(d1.valid_to, DATE '9999-12-31')
    ) AS overlap_rows;

    IF v_overlap_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены перекрытия SCD2-интервалов: %',
            v_overlap_count;
    END IF;

    -- Не более одной текущей версии на route_bk.
    SELECT COUNT(*)
    INTO v_dup_current
    FROM (
        SELECT route_bk
        FROM dds.dim_routes
        WHERE valid_to IS NULL
        GROUP BY route_bk
        HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_current <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены route_bk с > 1 текущей версией: %',
            v_dup_current;
    END IF;

    -- Покрытие ODS: все route_no имеют хотя бы одну версию в DDS.
    SELECT COUNT(*)
    INTO v_missing_count
    FROM (SELECT DISTINCT route_no FROM ods.routes) AS o
    WHERE NOT EXISTS (
        SELECT 1
        FROM dds.dim_routes AS d
        WHERE d.route_bk = o.route_no
    );

    IF v_missing_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes отсутствуют маршруты из ODS: %',
            v_missing_count;
    END IF;

    -- Current-срез DDS не содержит route_bk, которых нет в ODS.
    SELECT COUNT(*)
    INTO v_orphan_current
    FROM (
        SELECT DISTINCT route_bk
        FROM dds.dim_routes
        WHERE valid_to IS NULL
    ) AS d
    WHERE NOT EXISTS (
        SELECT 1
        FROM ods.routes AS o
        WHERE o.route_no = d.route_bk
    );

    IF v_orphan_current <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в current-срезе dds.dim_routes есть route_bk вне ODS: %',
            v_orphan_current;
    END IF;

    -- Обязательные поля.
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_routes
    WHERE route_sk IS NULL
        OR route_bk IS NULL
        OR route_bk = ''
        OR departure_airport IS NULL
        OR departure_airport = ''
        OR arrival_airport IS NULL
        OR arrival_airport = ''
        OR airplane_code IS NULL
        OR airplane_code = ''
        OR hashdiff IS NULL
        OR hashdiff = ''
        OR valid_from IS NULL
        OR created_at IS NULL
        OR updated_at IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_routes найдены NULL обязательные поля: %',
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.dim_routes ок, строк=% (версий)',
        v_row_count;
END $$;
