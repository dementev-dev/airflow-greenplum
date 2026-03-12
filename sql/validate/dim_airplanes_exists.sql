-- Проверка dds.dim_airplanes:
-- 1. Таблица не пуста
-- 2. Нет дублей по airplane_bk (SCD1 — UPSERT должен это гарантировать)
-- 3. Покрытие ODS: все airplane_code из ods.airplanes есть в измерении
-- 4. total_seats заполнен (вычисляется агрегацией из ods.seats; NULL = потерян JOIN)
DO $$
DECLARE
    v_count BIGINT;
    v_dup BIGINT;
    v_missing BIGINT;
    v_null_seats BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dds.dim_airplanes;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_airplanes пуста. Реализуйте загрузку: sql/dds/dim_airplanes_load.sql';
    END IF;

    SELECT COUNT(*) - COUNT(DISTINCT airplane_bk) INTO v_dup FROM dds.dim_airplanes;
    IF v_dup > 0 THEN
        RAISE EXCEPTION 'FAILED: dds.dim_airplanes содержит % дублей по airplane_bk. Проверьте SCD1-логику (UPSERT).', v_dup;
    END IF;

    SELECT COUNT(*) INTO v_missing
    FROM (SELECT DISTINCT airplane_code FROM ods.airplanes) AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_airplanes AS d WHERE d.airplane_bk = s.airplane_code
    );
    IF v_missing > 0 THEN
        RAISE EXCEPTION 'FAILED: % самолётов из ods.airplanes отсутствуют в dds.dim_airplanes.', v_missing;
    END IF;

    -- total_seats вычисляется через JOIN с ods.seats; если JOIN забыт — будет NULL
    SELECT COUNT(*) INTO v_null_seats
    FROM dds.dim_airplanes WHERE total_seats IS NULL;
    IF v_null_seats > 0 THEN
        RAISE EXCEPTION E'FAILED: % строк в dds.dim_airplanes имеют NULL в total_seats.\n'
            'Подсказка: total_seats считается агрегацией из ods.seats — проверьте JOIN в dim_airplanes_load.sql.', v_null_seats;
    END IF;

    RAISE NOTICE 'PASSED: dds.dim_airplanes содержит % строк, покрывает все BK из ODS, total_seats заполнен', v_count;
END $$;
