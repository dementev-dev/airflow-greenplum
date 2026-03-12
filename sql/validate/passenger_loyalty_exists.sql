-- Проверка dm.passenger_loyalty:
-- 1. Таблица не пуста
-- 2. Нет NULL в ключевом поле (passenger_sk)
DO $$
DECLARE
    v_count BIGINT;
    v_null_pk BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dm.passenger_loyalty;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dm.passenger_loyalty пуста. Реализуйте загрузку: sql/dm/passenger_loyalty_load.sql';
    END IF;

    SELECT COUNT(*) INTO v_null_pk
    FROM dm.passenger_loyalty
    WHERE passenger_sk IS NULL;

    IF v_null_pk > 0 THEN
        RAISE EXCEPTION 'FAILED: dm.passenger_loyalty содержит % строк с NULL в ключе (passenger_sk).', v_null_pk;
    END IF;

    RAISE NOTICE 'PASSED: dm.passenger_loyalty содержит % строк', v_count;
END $$;
