-- Проверка: ODS seats содержит все BK из STG-батча.
--
-- Аналог ods_airplanes_rowcount.sql, но для seats с составным BK (airplane_code, seat_no).
DO $$
DECLARE
    v_batch_count BIGINT;
    v_batch TEXT;
    v_ods_count BIGINT;
    v_missing_in_ods BIGINT;
    v_extra_in_ods BIGINT;
BEGIN
    -- ODS пуста?
    SELECT COUNT(*) INTO v_ods_count FROM ods.seats;
    IF v_ods_count = 0 THEN
        RAISE EXCEPTION 'FAILED: ods.seats пуста. Реализуйте загрузку: sql/ods/seats_load.sql';
    END IF;

    -- Инвариант: ODS после TRUNCATE+INSERT содержит ровно один _load_id
    SELECT COUNT(DISTINCT _load_id) INTO v_batch_count FROM ods.seats;
    IF v_batch_count <> 1 THEN
        RAISE EXCEPTION 'FAILED: ods.seats содержит % разных _load_id (ожидается 1 после TRUNCATE+INSERT). Проверьте, что load начинается с TRUNCATE.', v_batch_count;
    END IF;

    SELECT DISTINCT _load_id INTO v_batch FROM ods.seats;

    -- BK есть в STG-батче, но нет в ODS (потеряны при загрузке)
    SELECT COUNT(*) INTO v_missing_in_ods
    FROM (
        SELECT DISTINCT airplane_code, seat_no FROM stg.seats WHERE _load_id = v_batch
    ) AS stg_bk
    WHERE NOT EXISTS (
        SELECT 1 FROM ods.seats AS o
        WHERE o.airplane_code = stg_bk.airplane_code AND o.seat_no = stg_bk.seat_no
    );

    IF v_missing_in_ods > 0 THEN
        RAISE EXCEPTION 'FAILED: % мест из STG-батча (%) отсутствуют в ods.seats. Проверьте логику TRUNCATE+INSERT.', v_missing_in_ods, v_batch;
    END IF;

    -- BK есть в ODS, но нет в STG-батче
    SELECT COUNT(*) INTO v_extra_in_ods
    FROM (
        SELECT DISTINCT airplane_code, seat_no FROM ods.seats
    ) AS ods_bk
    WHERE NOT EXISTS (
        SELECT 1 FROM stg.seats AS s
        WHERE s._load_id = v_batch AND s.airplane_code = ods_bk.airplane_code AND s.seat_no = ods_bk.seat_no
    );

    IF v_extra_in_ods > 0 THEN
        RAISE EXCEPTION 'FAILED: % мест в ods.seats отсутствуют в STG-батче (%). Возможно, TRUNCATE не выполнился перед INSERT.', v_extra_in_ods, v_batch;
    END IF;

    RAISE NOTICE 'PASSED: ods.seats содержит % мест, множество BK = STG-батч %', v_ods_count, v_batch;
END $$;
