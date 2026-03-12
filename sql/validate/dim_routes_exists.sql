-- Проверка dds.dim_routes:
-- 1. Таблица не пуста
-- 2. Покрытие ODS: у каждого route_no из ods.routes есть открытая текущая версия (valid_to IS NULL)
-- 3. SCD2-инвариант: не более одной текущей версии на route_bk (valid_to IS NULL)
--
-- Почему покрытие проверяем через valid_to IS NULL, а не просто EXISTS:
-- закрытая версия (valid_to IS NOT NULL) без открытой означает «маршрут есть в ODS,
-- но в DDS только архив» — баг вида "старую версию закрыли, новую не вставили".
-- Такой маршрут теряется в point-in-time JOIN'ах и витринах.
DO $$
DECLARE
    v_count BIGINT;
    v_missing BIGINT;
    v_multi_current BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dds.dim_routes;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_routes пуста. Реализуйте загрузку: sql/dds/dim_routes_load.sql';
    END IF;

    -- Покрытие ODS: у каждого маршрута должна быть открытая (текущая) версия
    SELECT COUNT(*) INTO v_missing
    FROM (SELECT DISTINCT route_no FROM ods.routes) AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_routes AS d
        WHERE d.route_bk = s.route_no AND d.valid_to IS NULL
    );
    IF v_missing > 0 THEN
        RAISE EXCEPTION E'FAILED: % маршрутов из ods.routes не имеют текущей версии в dds.dim_routes (valid_to IS NULL).\n'
            'Подсказка: проверьте, что после UPDATE (закрытие старой версии) выполняется INSERT новой.', v_missing;
    END IF;

    -- SCD2-инвариант: не более одной текущей версии на route_bk
    SELECT COUNT(*) INTO v_multi_current
    FROM (
        SELECT route_bk FROM dds.dim_routes
        WHERE valid_to IS NULL
        GROUP BY route_bk HAVING COUNT(*) > 1
    ) AS d;

    IF v_multi_current > 0 THEN
        RAISE EXCEPTION E'FAILED: % маршрутов имеют более одной текущей версии (valid_to IS NULL).\n'
            'Подсказка: при INSERT новой версии проверяйте NOT EXISTS ... AND hashdiff = ...\n'
            'чтобы не создавать дубликат, если hashdiff не изменился.', v_multi_current;
    END IF;

    RAISE NOTICE 'PASSED: dds.dim_routes содержит % строк, покрывает ODS, по одной текущей версии на маршрут', v_count;
END $$;
