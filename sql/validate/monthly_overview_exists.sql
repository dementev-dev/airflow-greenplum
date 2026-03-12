-- Проверка dm.monthly_overview:
-- 1. Таблица не пуста
-- 2. Нет NULL в ключевых полях (year_actual, month_actual, airplane_sk)
DO $$
DECLARE
    v_count BIGINT;
    v_null_pk BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dm.monthly_overview;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dm.monthly_overview пуста. Реализуйте загрузку: sql/dm/monthly_overview_load.sql';
    END IF;

    SELECT COUNT(*) INTO v_null_pk
    FROM dm.monthly_overview
    WHERE year_actual IS NULL OR month_actual IS NULL OR airplane_sk IS NULL;

    IF v_null_pk > 0 THEN
        RAISE EXCEPTION 'FAILED: dm.monthly_overview содержит % строк с NULL в ключе (year_actual, month_actual, airplane_sk).', v_null_pk;
    END IF;

    RAISE NOTICE 'PASSED: dm.monthly_overview содержит % строк', v_count;
END $$;
