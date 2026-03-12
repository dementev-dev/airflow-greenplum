-- Проверка ODS-инвариантов:
-- 1. Нет дублей по BK в ods.airplanes и ods.seats.
-- 2. Обе таблицы загружены из одного согласованного батча (_load_id совпадают).
--
-- Почему важна согласованность батча:
-- ods.airplanes и ods.seats — части одного snapshot'а (TRUNCATE+INSERT из одного STG-батча).
-- Если они собраны из разных батчей (например, airplanes перезагрузили, а seats — нет),
-- JOIN между ними даст неконсистентный срез и невалидный total_seats в dim_airplanes.
DO $$
DECLARE
    v_dup_airplanes BIGINT;
    v_dup_seats BIGINT;
    v_load_id_airplanes TEXT;
    v_load_id_seats TEXT;
BEGIN
    -- airplanes: BK = airplane_code
    SELECT COUNT(*) INTO v_dup_airplanes
    FROM (
        SELECT airplane_code FROM ods.airplanes
        GROUP BY airplane_code HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_airplanes > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes содержит % дублирующихся airplane_code. Проверьте, что load начинается с TRUNCATE.', v_dup_airplanes;
    END IF;

    -- seats: BK = (airplane_code, seat_no)
    SELECT COUNT(*) INTO v_dup_seats
    FROM (
        SELECT airplane_code, seat_no FROM ods.seats
        GROUP BY airplane_code, seat_no HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_seats > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.seats содержит % дублирующихся (airplane_code, seat_no). Проверьте, что load начинается с TRUNCATE.', v_dup_seats;
    END IF;

    -- Согласованность батча: обе таблицы должны быть загружены из одного _load_id
    SELECT DISTINCT _load_id INTO v_load_id_airplanes FROM ods.airplanes;
    SELECT DISTINCT _load_id INTO v_load_id_seats     FROM ods.seats;

    IF v_load_id_airplanes IS NOT NULL
       AND v_load_id_seats IS NOT NULL
       AND v_load_id_airplanes <> v_load_id_seats
    THEN
        RAISE EXCEPTION E'FAILED: ods.airplanes и ods.seats загружены из разных батчей.\n'
            '  airplanes._load_id = %\n'
            '  seats._load_id     = %\n'
            'ODS — единый snapshot: обе таблицы должны содержать один и тот же _load_id.\n'
            'Запустите оба load-скрипта (airplanes_load.sql и seats_load.sql) в одном прогоне.',
            v_load_id_airplanes, v_load_id_seats;
    END IF;

    RAISE NOTICE 'PASSED: Нет дублей по BK в ods.airplanes и ods.seats; батч согласован (%)', v_load_id_airplanes;
END $$;
