-- Проверка dm.airport_traffic:
-- 1. Таблица не пуста
-- 2. Нет NULL в ключевых полях (traffic_date, airport_sk)
DO $$
DECLARE
    v_count BIGINT;
    v_null_pk BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dm.airport_traffic;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dm.airport_traffic пуста. Реализуйте загрузку: sql/dm/airport_traffic_load.sql';
    END IF;

    SELECT COUNT(*) INTO v_null_pk
    FROM dm.airport_traffic
    WHERE traffic_date IS NULL OR airport_sk IS NULL;

    IF v_null_pk > 0 THEN
        RAISE EXCEPTION 'FAILED: dm.airport_traffic содержит % строк с NULL в ключе (traffic_date, airport_sk).', v_null_pk;
    END IF;

    RAISE NOTICE 'PASSED: dm.airport_traffic содержит % строк', v_count;
END $$;
