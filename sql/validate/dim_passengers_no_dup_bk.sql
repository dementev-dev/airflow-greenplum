-- Проверка: нет дублей по passenger_id в dds.dim_passengers.
--
-- Отдельный таск для точной диагностики — студент сразу видит причину проблемы.
-- Типичная ошибка: INSERT без проверки EXISTS при SCD1-загрузке.
DO $$
DECLARE
    v_dup BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_dup
    FROM (
        SELECT passenger_id FROM dds.dim_passengers
        GROUP BY passenger_id HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup > 0 THEN
        RAISE EXCEPTION E'FAILED: dds.dim_passengers содержит % дублирующихся passenger_id.\n'
            'Подсказка: при SCD1 нужно INSERT ... WHERE NOT EXISTS или ON CONFLICT DO NOTHING/UPDATE.\n'
            'Если таск check_dim_passengers_exists тоже упал — начните с него.', v_dup;
    END IF;

    RAISE NOTICE 'PASSED: Нет дублей по passenger_id в dds.dim_passengers';
END $$;
