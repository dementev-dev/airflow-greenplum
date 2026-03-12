-- Проверка: ODS airplanes содержит все BK из STG-батча.
--
-- Логика: ODS — TRUNCATE+INSERT snapshot. STG — append-only история всех батчей.
-- Сравниваем не счётчики строк, а точное множество BK для того батча,
-- который ODS фактически загрузил (определяем по _load_id из ods.airplanes).
DO $$
DECLARE
    v_batch_count BIGINT;
    v_batch TEXT;
    v_ods_count BIGINT;
    v_missing_in_ods BIGINT;
    v_extra_in_ods BIGINT;
BEGIN
    -- ODS пуста?
    SELECT COUNT(*) INTO v_ods_count FROM ods.airplanes;
    IF v_ods_count = 0 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes пуста. Реализуйте загрузку: sql/ods/airplanes_load.sql';
    END IF;

    -- Инвариант: ODS после TRUNCATE+INSERT содержит ровно один _load_id
    SELECT COUNT(DISTINCT _load_id) INTO v_batch_count FROM ods.airplanes;
    IF v_batch_count <> 1 THEN
        RAISE EXCEPTION 'FAILED: ods.airplanes содержит % разных _load_id (ожидается 1 после TRUNCATE+INSERT). Проверьте, что load начинается с TRUNCATE.', v_batch_count;
    END IF;

    -- Определяем батч, из которого загружена ODS
    SELECT DISTINCT _load_id INTO v_batch FROM ods.airplanes;

    -- BK есть в STG-батче, но нет в ODS (потеряны при загрузке)
    SELECT COUNT(*) INTO v_missing_in_ods
    FROM (
        SELECT DISTINCT airplane_code FROM stg.airplanes WHERE _load_id = v_batch
    ) AS stg_bk
    WHERE NOT EXISTS (
        SELECT 1 FROM ods.airplanes AS o WHERE o.airplane_code = stg_bk.airplane_code
    );

    IF v_missing_in_ods > 0 THEN
        RAISE EXCEPTION 'FAILED: % самолётов из STG-батча (%) отсутствуют в ods.airplanes. Проверьте логику TRUNCATE+INSERT.', v_missing_in_ods, v_batch;
    END IF;

    -- BK есть в ODS, но нет в STG-батче (откуда взялись?)
    SELECT COUNT(*) INTO v_extra_in_ods
    FROM (
        SELECT DISTINCT airplane_code FROM ods.airplanes
    ) AS ods_bk
    WHERE NOT EXISTS (
        SELECT 1 FROM stg.airplanes AS s WHERE s._load_id = v_batch AND s.airplane_code = ods_bk.airplane_code
    );

    IF v_extra_in_ods > 0 THEN
        RAISE EXCEPTION 'FAILED: % самолётов в ods.airplanes отсутствуют в STG-батче (%). Возможно, TRUNCATE не выполнился перед INSERT.', v_extra_in_ods, v_batch;
    END IF;

    RAISE NOTICE 'PASSED: ods.airplanes содержит % самолётов, множество BK = STG-батч %', v_ods_count, v_batch;
END $$;
