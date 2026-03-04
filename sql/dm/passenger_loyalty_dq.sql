-- DQ проверки для витрины dm.passenger_loyalty.
--
-- Учебные цели:
-- 1. Валидация бизнес-логики дат (стаж не может быть отрицательным).
-- 2. Учебная проверка ссылочной целостности (FK Integrity).

DO $$
DECLARE
    row_count INTEGER;
    duplicate_count INTEGER;
    invalid_dates_count INTEGER;
    orphan_keys_count INTEGER;
BEGIN
    -- 1. Проверка на наполненность
    SELECT COUNT(*) INTO row_count FROM dm.passenger_loyalty;
    IF row_count = 0 THEN
        RAISE EXCEPTION 'DQ Error: Таблица dm.passenger_loyalty пуста.';
    END IF;

    -- 2. Проверка на уникальность (зерно - passenger_sk)
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT passenger_sk FROM dm.passenger_loyalty
        GROUP BY passenger_sk HAVING COUNT(*) > 1
    ) q;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.passenger_loyalty обнаружены дубликаты по passenger_sk (% шт).', duplicate_count;
    END IF;

    -- 3. Проверка логики дат
    SELECT COUNT(*) INTO invalid_dates_count
    FROM dm.passenger_loyalty
    WHERE first_flight_date > last_flight_date;

    IF invalid_dates_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.passenger_loyalty обнаружены записи с first_date > last_date (% строк).', invalid_dates_count;
    END IF;

    -- 4. Учебная проверка ссылочной целостности (FK Check)
    -- В продакшене это обычно гарантируется JOIN при загрузке, но здесь мы показываем саму возможность проверки.
    SELECT COUNT(*) INTO orphan_keys_count
    FROM dm.passenger_loyalty tgt
    LEFT JOIN dds.dim_passengers p ON tgt.passenger_sk = p.passenger_sk
    WHERE p.passenger_sk IS NULL;

    IF orphan_keys_count > 0 THEN
        RAISE EXCEPTION 'DQ Error: В dm.passenger_loyalty обнаружены пассажиры, отсутствующие в dim_passengers (% строк).', orphan_keys_count;
    END IF;

    RAISE NOTICE 'DQ Success: dm.passenger_loyalty успешно прошла все проверки (% строк).', row_count;
END $$;
