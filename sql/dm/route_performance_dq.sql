-- DQ проверки для витрины dm.route_performance.
--
-- Учебные цели:
-- 1. Использование PL/pgSQL блоков (DO $$ ...) для кастомных проверок.
-- 2. Валидация бизнес-инвариантов после загрузки Full Rebuild.

DO $$
DECLARE
    row_count INTEGER;
    duplicate_count INTEGER;
    invalid_metrics_count INTEGER;
    null_attributes_count INTEGER;
BEGIN
    -- 1. Проверка на наполненность
    SELECT COUNT(*) INTO row_count FROM dm.route_performance;
    IF row_count = 0 THEN
        RAISE EXCEPTION 'DQ Error: Таблица dm.route_performance пуста после загрузки.';
    END IF;

    -- 2. Проверка на уникальность бизнес-ключа
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT route_bk FROM dm.route_performance
        GROUP BY route_bk HAVING COUNT(*) > 1
    ) AS q;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.route_performance обнаружены дубликаты по route_bk (% шт).', duplicate_count;
    END IF;

    -- 3. Проверка бизнес-метрик (инварианты)
    -- Load factor не может быть отрицательным или физически невозможным (например, более 200%)
    SELECT COUNT(*) INTO invalid_metrics_count
    FROM dm.route_performance
    WHERE avg_load_factor < 0 OR avg_load_factor > 2.0;

    IF invalid_metrics_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.route_performance обнаружены некорректные показатели load factor (% строк).', invalid_metrics_count;
    END IF;

    -- 4. Проверка обязательных атрибутов на NULL
    SELECT COUNT(*) INTO null_attributes_count
    FROM dm.route_performance
    WHERE departure_city IS NULL OR arrival_city IS NULL OR airplane_model IS NULL;

    IF null_attributes_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.route_performance обнаружены NULL-значения в обязательных полях (% строк).', null_attributes_count;
    END IF;

    -- 5. Проверка: посаженных не может быть больше, чем купивших билет
    SELECT COUNT(*) INTO invalid_metrics_count
    FROM dm.route_performance
    WHERE total_boarded > total_tickets;

    IF invalid_metrics_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.route_performance total_boarded > total_tickets (% строк).', invalid_metrics_count;
    END IF;

    RAISE NOTICE 'DQ Success: dm.route_performance успешно прошла все проверки (% строк).', row_count;
END $$;
