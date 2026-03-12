-- DQ проверки для витрины dm.monthly_overview.
--
-- Учебные цели:
-- 1. Валидация логики двухуровневой агрегации: load factor не должен превышать 1.0
--    (в отличие от route_performance, где допускалась погрешность из-за упрощенного расчета).
-- 2. Базовые санити-проверки для дат (month BETWEEN 1 AND 12).

DO $$
DECLARE
    row_count INTEGER;
    duplicate_count INTEGER;
    invalid_month_count INTEGER;
    invalid_load_factor_count INTEGER;
BEGIN
    -- 1. Проверка на наполненность
    SELECT COUNT(*) INTO row_count FROM dm.monthly_overview;
    IF row_count = 0 THEN
        RAISE EXCEPTION 'DQ Error: Таблица dm.monthly_overview пуста.';
    END IF;

    -- 2. Проверка на уникальность (зерно - год + месяц + самолет)
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT year_actual, month_actual, airplane_sk FROM dm.monthly_overview
        GROUP BY year_actual, month_actual, airplane_sk HAVING COUNT(*) > 1
    ) q;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.monthly_overview обнаружены дубликаты по (year, month, airplane_sk) (% шт).', duplicate_count;
    END IF;

    -- 3. Санити-проверка месяца
    SELECT COUNT(*) INTO invalid_month_count
    FROM dm.monthly_overview
    WHERE month_actual < 1 OR month_actual > 12;

    IF invalid_month_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.monthly_overview обнаружены некорректные месяцы (% строк).', invalid_month_count;
    END IF;

    -- 4. Проверка точного load_factor
    -- В этой витрине мы считаем его честно (по рейсам), поэтому он строго <= 1.0
    -- (исключая экзотические случаи овербукинга, но для учебного стенда ставим жесткий лимит 1.0).
    SELECT COUNT(*) INTO invalid_load_factor_count
    FROM dm.monthly_overview
    WHERE avg_load_factor < 0 OR avg_load_factor > 1.0;

    IF invalid_load_factor_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.monthly_overview обнаружен avg_load_factor вне диапазона 0..1 (% строк).', invalid_load_factor_count;
    END IF;

    RAISE NOTICE 'DQ Success: dm.monthly_overview успешно прошла все проверки (% строк).', row_count;
END $$;
