-- DQ для DDS dim_calendar.

DO $$
DECLARE
    v_row_count BIGINT;
    v_dup_sk BIGINT;
    v_dup_date BIGINT;
    v_null_count BIGINT;
    v_missing_flight_dates BIGINT;
BEGIN
    -- Таблица должна быть достаточно заполнена.
    SELECT COUNT(*)
    INTO v_row_count
    FROM dds.dim_calendar;

    IF v_row_count < 1000 THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.dim_calendar содержит слишком мало строк: %',
            v_row_count;
    END IF;

    -- Нет дублей по surrogate key.
    SELECT COUNT(*) - COUNT(DISTINCT calendar_sk)
    INTO v_dup_sk
    FROM dds.dim_calendar;

    IF v_dup_sk <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_calendar найдены дубликаты calendar_sk: %',
            v_dup_sk;
    END IF;

    -- Нет дублей по business key (date_actual).
    SELECT COUNT(*) - COUNT(DISTINCT date_actual)
    INTO v_dup_date
    FROM dds.dim_calendar;

    IF v_dup_date <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_calendar найдены дубликаты date_actual: %',
            v_dup_date;
    END IF;

    -- Обязательные поля не NULL.
    SELECT COUNT(*)
    INTO v_null_count
    FROM dds.dim_calendar
    WHERE calendar_sk IS NULL
        OR date_actual IS NULL
        OR year_actual IS NULL
        OR month_actual IS NULL
        OR day_actual IS NULL
        OR day_of_week IS NULL
        OR day_name IS NULL
        OR day_name = ''
        OR is_weekend IS NULL;

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.dim_calendar найдены NULL/пустые обязательные поля: %',
            v_null_count;
    END IF;

    -- Календарь покрывает даты вылета из ODS (где scheduled_departure не NULL).
    SELECT COUNT(*)
    INTO v_missing_flight_dates
    FROM (
        SELECT DISTINCT f.scheduled_departure::DATE AS departure_date
        FROM ods.flights AS f
        WHERE f.scheduled_departure IS NOT NULL
    ) AS src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dds.dim_calendar AS c
        WHERE c.date_actual = src.departure_date
    );

    IF v_missing_flight_dates <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.dim_calendar не покрывает даты вылета из ods.flights: %',
            v_missing_flight_dates;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.dim_calendar ок, строк=%',
        v_row_count;
END $$;
