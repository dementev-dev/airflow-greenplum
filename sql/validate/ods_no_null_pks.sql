-- Проверка: PK-поля не содержат NULL в ods.airplanes и ods.seats.
--
-- NULL в PK ломает JOIN'ы и агрегаты в DDS/DM — такие строки «теряются» тихо.
DO $$
DECLARE
    v_null_airplanes BIGINT;
    v_null_seats BIGINT;
BEGIN
    -- airplanes: PK = airplane_code
    SELECT COUNT(*) INTO v_null_airplanes
    FROM ods.airplanes WHERE airplane_code IS NULL;

    IF v_null_airplanes > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes содержит % строк с NULL в airplane_code.', v_null_airplanes;
    END IF;

    -- seats: PK = (airplane_code, seat_no)
    SELECT COUNT(*) INTO v_null_seats
    FROM ods.seats WHERE airplane_code IS NULL OR seat_no IS NULL;

    IF v_null_seats > 0 THEN
        RAISE EXCEPTION 'FAILED: ods.seats содержит % строк с NULL в PK (airplane_code, seat_no).', v_null_seats;
    END IF;

    RAISE NOTICE 'PASSED: PK не содержат NULL в ods.airplanes и ods.seats';
END $$;
