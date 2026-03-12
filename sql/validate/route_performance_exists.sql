-- Проверка dm.route_performance:
-- 1. Таблица не пуста
-- 2. Нет NULL в ключевом поле (route_bk)
DO $$
DECLARE
    v_count BIGINT;
    v_null_pk BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dm.route_performance;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dm.route_performance пуста. Реализуйте загрузку: sql/dm/route_performance_load.sql';
    END IF;

    SELECT COUNT(*) INTO v_null_pk
    FROM dm.route_performance
    WHERE route_bk IS NULL;

    IF v_null_pk > 0 THEN
        RAISE EXCEPTION 'FAILED: dm.route_performance содержит % строк с NULL в ключе (route_bk).', v_null_pk;
    END IF;

    RAISE NOTICE 'PASSED: dm.route_performance содержит % строк', v_count;
END $$;
