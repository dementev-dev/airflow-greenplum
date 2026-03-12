-- DQ для DDS fact_flight_sales.

DO $$
DECLARE
    v_row_count BIGINT;
    v_ods_count BIGINT;
    v_dup_count BIGINT;
    v_null_passenger BIGINT;
    v_null_tariff BIGINT;
    v_null_route_related BIGINT;
    v_null_calendar BIGINT;
    v_null_required BIGINT;
BEGIN
    -- Для пустого инкрементального окна (ods.segments) допускаем пустой факт.
    SELECT COUNT(*)
    INTO v_ods_count
    FROM ods.segments;

    SELECT COUNT(*)
    INTO v_row_count
    FROM dds.fact_flight_sales;

    IF v_ods_count = 0 THEN
        IF v_row_count <> 0 THEN
            RAISE EXCEPTION
                'DQ FAILED: ods.segments пустая, но в dds.fact_flight_sales есть строки: %',
                v_row_count;
        END IF;

        RAISE NOTICE
            'DQ PASSED: ods.segments и dds.fact_flight_sales пустые (инкрементальное окно без сегментов).';
        RETURN;
    END IF;

    IF v_row_count = 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.fact_flight_sales пуста при непустом источнике ods.segments (%).',
            v_ods_count;
    END IF;

    -- Покрытие: количество строк = ods.segments.
    IF v_row_count <> v_ods_count THEN
        RAISE EXCEPTION
            'DQ FAILED: dds.fact_flight_sales (%) <> ods.segments (%). Потеряны строки.',
            v_row_count,
            v_ods_count;
    END IF;

    -- Нет дублей по зерну.
    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT ticket_no, flight_id
        FROM dds.fact_flight_sales
        GROUP BY ticket_no, flight_id
        HAVING COUNT(*) > 1
    ) AS d;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в dds.fact_flight_sales дубликаты (ticket_no, flight_id): %',
            v_dup_count;
    END IF;

    -- Ссылочная целостность: passenger_sk.
    SELECT COUNT(*)
    INTO v_null_passenger
    FROM dds.fact_flight_sales
    WHERE passenger_sk IS NULL;

    IF v_null_passenger <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в fact_flight_sales строки без passenger_sk: %',
            v_null_passenger;
    END IF;

    -- Ссылочная целостность: tariff_sk.
    SELECT COUNT(*)
    INTO v_null_tariff
    FROM dds.fact_flight_sales
    WHERE tariff_sk IS NULL;

    IF v_null_tariff <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в fact_flight_sales строки без tariff_sk: %',
            v_null_tariff;
    END IF;

    -- Route-related FK: допустимо при аномалиях, фейлим если > 1%.
    SELECT COUNT(*)
    INTO v_null_route_related
    FROM dds.fact_flight_sales
    WHERE route_sk IS NULL
        OR departure_airport_sk IS NULL
        OR arrival_airport_sk IS NULL
        OR airplane_sk IS NULL;

    IF v_null_route_related > 0 THEN
        IF v_null_route_related * 100.0 / NULLIF(v_row_count, 0) > 1.0 THEN
            RAISE EXCEPTION
                'DQ FAILED: в fact_flight_sales слишком много строк с NULL в route-related FK: % (>1%%)',
                v_null_route_related;
        ELSE
            RAISE NOTICE
                'DQ WARNING: в fact_flight_sales строк с NULL в route-related FK: % (<=1%%, допустимо)',
                v_null_route_related;
        END IF;
    END IF;

    -- Calendar: допустимо если scheduled_departure IS NULL, фейлим если > 1%.
    SELECT COUNT(*)
    INTO v_null_calendar
    FROM dds.fact_flight_sales
    WHERE calendar_sk IS NULL;

    IF v_null_calendar > 0 THEN
        IF v_null_calendar * 100.0 / NULLIF(v_row_count, 0) > 1.0 THEN
            RAISE EXCEPTION
                'DQ FAILED: в fact_flight_sales слишком много строк без calendar_sk: % (>1%%)',
                v_null_calendar;
        ELSE
            RAISE NOTICE
                'DQ WARNING: в fact_flight_sales строк без calendar_sk: % (<=1%%, допустимо)',
                v_null_calendar;
        END IF;
    END IF;

    -- Обязательные поля.
    SELECT COUNT(*)
    INTO v_null_required
    FROM dds.fact_flight_sales
    WHERE book_ref IS NULL
        OR book_ref = ''
        OR ticket_no IS NULL
        OR ticket_no = ''
        OR flight_id IS NULL
        OR is_boarded IS NULL
        OR _load_id IS NULL
        OR _load_id = ''
        OR _load_ts IS NULL;

    IF v_null_required <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: в fact_flight_sales NULL обязательные поля: %',
            v_null_required;
    END IF;

    RAISE NOTICE
        'DQ PASSED: dds.fact_flight_sales ок, строк=%',
        v_row_count;
END $$;
