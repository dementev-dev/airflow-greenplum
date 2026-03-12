-- Проверка: нет «дыр» в SCD2-интервалах dim_routes.
--
-- Для каждого route_bk с несколькими версиями проверяем, что
-- valid_to предыдущей версии = valid_from следующей.
--
-- Важно: если маршрут «исчез» из ODS, его текущая версия закрывается
-- (valid_to = CURRENT_DATE), но новая не вставляется — это корректно.
-- Проверяем «дыру» только если следующая версия существует.
DO $$
DECLARE
    v_gaps BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_gaps
    FROM (
        SELECT route_bk, valid_to,
               LEAD(valid_from) OVER (PARTITION BY route_bk ORDER BY valid_from) AS next_valid_from
        FROM dds.dim_routes
    ) AS t
    WHERE valid_to IS NOT NULL
      AND next_valid_from IS NOT NULL   -- следующая версия существует (не «исчезнувший» маршрут)
      AND valid_to <> next_valid_from;

    IF v_gaps > 0 THEN
        RAISE EXCEPTION 'FAILED: В dds.dim_routes найдено % «дыр» между версиями SCD2. valid_to старой версии должен совпадать с valid_from новой.', v_gaps;
    END IF;

    RAISE NOTICE 'PASSED: Нет «дыр» в SCD2-версиях dim_routes';
END $$;
