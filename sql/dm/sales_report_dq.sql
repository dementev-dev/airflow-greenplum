-- DQ для DM витрины sales_report.
--
-- Проверки:
-- 1. Таблица не пуста (если источник за затронутые батчем дни не пуст)
-- 2. Нет дублей по составному ключу
-- 3. Бизнес-инварианты: tickets_sold >= passengers_boarded, boarding_rate BETWEEN 0 AND 1
-- 4. Обязательные поля не NULL
--
-- ВАЖНО (Паттерн Data Quality):
-- Проверки выполняются ИНКРЕМЕНТАЛЬНО. Мы спрашиваем саму витрину, какие даты
-- были обновлены текущим запуском DAG-а (по _load_id = run_id), и валидируем
-- только эти даты. Это корректно, потому что на шаге load мы записали
-- текущий run_id DM-DAG-а в обновлённые строки витрины.

DO $$
DECLARE
    v_row_count BIGINT;
    v_src_count BIGINT;
    v_dup_count BIGINT;
    v_invalid_boarding BIGINT;
    v_null_required BIGINT;
BEGIN
    -- Создаем временную таблицу с датами, которые МЫ обновили в этом запуске DAG-а.
    -- Спрашиваем саму витрину (не DDS!), потому что на шаге load мы записали
    -- _load_id = '{{ run_id }}' текущего DM-DAG-а в обновлённые строки.
    CREATE TEMP TABLE tmp_dq_affected_dates ON COMMIT DROP AS
    SELECT DISTINCT flight_date AS date_actual
    FROM dm.sales_report
    WHERE _load_id = '{{ run_id }}';

    -- Если батч пуст (ничего не обновлялось), нам нечего валидировать.
    IF NOT EXISTS (SELECT 1 FROM tmp_dq_affected_dates) THEN
        RAISE NOTICE 'DQ PASSED: Витрина не обновлялась в этом запуске (%). Новых данных в DDS нет.', '{{ run_id }}';
        RETURN;
    END IF;

    -- Проверка 1: Таблица не пуста при непустом источнике (для затронутых дат)
    SELECT COUNT(*)
    INTO v_src_count
    FROM dds.fact_flight_sales AS f
    JOIN dds.dim_calendar AS cal
        ON cal.calendar_sk = f.calendar_sk
    WHERE cal.date_actual IN (SELECT date_actual FROM tmp_dq_affected_dates);

    SELECT COUNT(*)
    INTO v_row_count
    FROM dm.sales_report
    WHERE flight_date IN (SELECT date_actual FROM tmp_dq_affected_dates);

    IF v_src_count > 0 AND v_row_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: dm.sales_report пуста для дат батча при непустом источнике (% строк в fact_flight_sales)',
            v_src_count;
    END IF;

    RAISE NOTICE 'DQ INFO: dm.sales_report содержит % строк для дат текущего батча', v_row_count;

    -- Проверка 2: Нет дублей по составному ключу (в рамках затронутых дат)
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk
        FROM dm.sales_report
        WHERE flight_date IN (SELECT date_actual FROM tmp_dq_affected_dates)
        GROUP BY flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk
        HAVING COUNT(*) > 1
    ) AS dups;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: Найдено % дублирующихся комбинаций ключа в dm.sales_report',
            v_dup_count;
    END IF;

    RAISE NOTICE 'DQ PASSED: Дублей по составному ключу для дат батча нет';

    -- Проверка 3: Бизнес-инварианты (в рамках затронутых дат)

    -- tickets_sold >= passengers_boarded
    SELECT COUNT(*)
    INTO v_invalid_boarding
    FROM dm.sales_report
    WHERE flight_date IN (SELECT date_actual FROM tmp_dq_affected_dates)
      AND tickets_sold < passengers_boarded;

    IF v_invalid_boarding <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с tickets_sold < passengers_boarded',
            v_invalid_boarding;
    END IF;

    -- boarding_rate BETWEEN 0 AND 1
    SELECT COUNT(*)
    INTO v_invalid_boarding
    FROM dm.sales_report
    WHERE flight_date IN (SELECT date_actual FROM tmp_dq_affected_dates)
      AND (boarding_rate < 0 OR boarding_rate > 1);

    IF v_invalid_boarding <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с boarding_rate вне диапазона [0, 1]',
            v_invalid_boarding;
    END IF;

    -- total_revenue >= 0
    SELECT COUNT(*)
    INTO v_invalid_boarding
    FROM dm.sales_report
    WHERE flight_date IN (SELECT date_actual FROM tmp_dq_affected_dates)
      AND total_revenue < 0;

    IF v_invalid_boarding <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с отрицательной total_revenue',
            v_invalid_boarding;
    END IF;

    RAISE NOTICE 'DQ PASSED: Бизнес-инварианты соблюдены';

    -- Проверка 4: Обязательные поля не NULL (в рамках затронутых дат)
    SELECT COUNT(*)
    INTO v_null_required
    FROM dm.sales_report
    WHERE flight_date IN (SELECT date_actual FROM tmp_dq_affected_dates)
      AND (
          departure_airport_sk IS NULL
          OR arrival_airport_sk IS NULL
          OR tariff_sk IS NULL
          OR tickets_sold IS NULL
          OR total_revenue IS NULL
          OR boarding_rate IS NULL
      );

    IF v_null_required <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: % строк с NULL в обязательных полях',
            v_null_required;
    END IF;

    RAISE NOTICE 'DQ PASSED: Обязательные поля заполнены';

    -- Итог
    RAISE NOTICE 'DQ COMPLETE: dm.sales_report прошла все проверки для текущего батча';
END $$;
