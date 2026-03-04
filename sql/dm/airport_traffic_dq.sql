-- DQ проверки для витрины dm.airport_traffic.
--
-- Учебные цели:
-- 1. Проверка арифметических инвариантов (сумма частей должна быть равна целому).
-- 2. Валидация уникальности составного ключа (зерна).

DO $$
DECLARE
    row_count INTEGER;
    duplicate_count INTEGER;
    invalid_total_count INTEGER;
    negative_metrics_count INTEGER;
BEGIN
    -- 1. Проверка на наполненность
    SELECT COUNT(*) INTO row_count FROM dm.airport_traffic;
    IF row_count = 0 THEN
        RAISE EXCEPTION 'DQ Error: Таблица dm.airport_traffic пуста.';
    END IF;

    -- 2. Проверка на уникальность (зерно - traffic_date + airport_sk)
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT traffic_date, airport_sk FROM dm.airport_traffic
        GROUP BY traffic_date, airport_sk HAVING COUNT(*) > 1
    ) q;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.airport_traffic обнаружены дубликаты по (date, airport_sk) (% шт).', duplicate_count;
    END IF;

    -- 3. Проверка инварианта total = departures + arrivals
    SELECT COUNT(*) INTO invalid_total_count
    FROM dm.airport_traffic
    WHERE total_passengers != (departures_passengers + arrivals_passengers);

    IF invalid_total_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.airport_traffic нарушен инвариант total_passengers = dep + arr (% строк).', invalid_total_count;
    END IF;

    -- 4. Проверка на отрицательные значения
    SELECT COUNT(*) INTO negative_metrics_count
    FROM dm.airport_traffic
    WHERE departures_flights < 0 OR arrivals_flights < 0 OR departures_revenue < 0 OR arrivals_revenue < 0;

    IF negative_metrics_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.airport_traffic обнаружены отрицательные метрики (% строк).', negative_metrics_count;
    END IF;

    RAISE NOTICE 'DQ Success: dm.airport_traffic успешно прошла все проверки (% строк).', row_count;
END $$;
