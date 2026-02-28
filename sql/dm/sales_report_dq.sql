-- DQ для DM витрины sales_report.
--
-- Проверки:
-- 1. Таблица не пуста (если источник не пуст)
-- 2. Нет дублей по составному ключу (flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk)
-- 3. Бизнес-инварианты: tickets_sold >= passengers_boarded, boarding_rate BETWEEN 0 AND 1
-- 4. Обязательные поля не NULL

DO $$
DECLARE
    v_row_count BIGINT;
    v_src_count BIGINT;
    v_dup_count BIGINT;
    v_invalid_boarding BIGINT;
    v_null_required BIGINT;
BEGIN
    -- Проверка 1: Таблица не пуста при непустом источнике
    SELECT COUNT(*)
    INTO v_src_count
    FROM dds.fact_flight_sales;

    SELECT COUNT(*)
    INTO v_row_count
    FROM dm.sales_report;

    -- Если источник пуст, допускаем пустую витрину (инкрементальное окно без данных)
    IF v_src_count = 0 THEN
        IF v_row_count <> 0 THEN
            RAISE EXCEPTION
                'DQ FAILED: fact_flight_sales пуст, но dm.sales_report содержит % строк',
                v_row_count;
        END IF;
        RAISE NOTICE 'DQ PASSED: Источник и витрина пусты (нет данных для обработки).';
        RETURN;
    END IF;

    IF v_row_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: dm.sales_report пуста при непустом источнике (% строк в fact_flight_sales)',
            v_src_count;
    END IF;

    RAISE NOTICE 'DQ INFO: dm.sales_report содержит % строк', v_row_count;

    -- Проверка 2: Нет дублей по составному ключу
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk
        FROM dm.sales_report
        GROUP BY flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk
        HAVING COUNT(*) > 1
    ) AS dups;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: Найдено % дублирующихся комбинаций ключа в dm.sales_report',
            v_dup_count;
    END IF;

    RAISE NOTICE 'DQ PASSED: Дублей по составному ключу нет';

    -- Проверка 3: Бизнес-инварианты

    -- tickets_sold >= passengers_boarded
    SELECT COUNT(*)
    INTO v_invalid_boarding
    FROM dm.sales_report
    WHERE tickets_sold < passengers_boarded;

    IF v_invalid_boarding <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с tickets_sold < passengers_boarded',
            v_invalid_boarding;
    END IF;

    -- boarding_rate BETWEEN 0 AND 1
    SELECT COUNT(*)
    INTO v_invalid_boarding
    FROM dm.sales_report
    WHERE boarding_rate < 0 OR boarding_rate > 1;

    IF v_invalid_boarding <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с boarding_rate вне диапазона [0, 1]',
            v_invalid_boarding;
    END IF;

    -- total_revenue >= 0
    SELECT COUNT(*)
    INTO v_invalid_boarding
    FROM dm.sales_report
    WHERE total_revenue < 0;

    IF v_invalid_boarding <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с отрицательной total_revenue',
            v_invalid_boarding;
    END IF;

    RAISE NOTICE 'DQ PASSED: Бизнес-инварианты соблюдены';

    -- Проверка 4: Обязательные поля не NULL
    SELECT COUNT(*)
    INTO v_null_required
    FROM dm.sales_report
    WHERE flight_date IS NULL
       OR departure_airport_sk IS NULL
       OR arrival_airport_sk IS NULL
       OR tariff_sk IS NULL
       OR tickets_sold IS NULL
       OR total_revenue IS NULL
       OR boarding_rate IS NULL;

    IF v_null_required <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с NULL в обязательных полях',
            v_null_required;
    END IF;

    RAISE NOTICE 'DQ PASSED: Обязательные поля заполнены';

    -- Итог
    RAISE NOTICE 'DQ COMPLETE: dm.sales_report прошла все проверки';
END $$;
