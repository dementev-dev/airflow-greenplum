-- Проверка dds.dim_passengers:
-- 1. Таблица не пуста
-- 2. Нет дублей по passenger_id (SCD1 — типичная ошибка: INSERT без EXISTS)
-- 3. Покрытие ODS: все уникальные passenger_id из ods.tickets есть в измерении
DO $$
DECLARE
    v_count BIGINT;
    v_dup BIGINT;
    v_missing BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dds.dim_passengers;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_passengers пуста. Реализуйте загрузку: sql/dds/dim_passengers_load.sql';
    END IF;

    SELECT COUNT(*) - COUNT(DISTINCT passenger_id) INTO v_dup FROM dds.dim_passengers;
    IF v_dup > 0 THEN
        RAISE EXCEPTION E'FAILED: dds.dim_passengers содержит % дублей по passenger_id.\n'
            'Подсказка: INSERT без проверки EXISTS создаёт дубли при повторном запуске.\n'
            'Используйте INSERT ... WHERE NOT EXISTS или ON CONFLICT DO UPDATE.', v_dup;
    END IF;

    SELECT COUNT(*) INTO v_missing
    FROM (SELECT DISTINCT passenger_id FROM ods.tickets) AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_passengers AS d WHERE d.passenger_id = s.passenger_id
    );
    IF v_missing > 0 THEN
        RAISE EXCEPTION 'FAILED: % пассажиров из ods.tickets отсутствуют в dds.dim_passengers.', v_missing;
    END IF;

    RAISE NOTICE 'PASSED: dds.dim_passengers содержит % строк, нет дублей, покрывает все BK из ODS', v_count;
END $$;
