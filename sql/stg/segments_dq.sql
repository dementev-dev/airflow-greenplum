-- Проверки качества данных для segments

DO $$
DECLARE
    v_batch_id        TEXT      := '{{ run_id }}'::text;
    v_prev_ts         TIMESTAMP;
    v_src_count       BIGINT;
    v_stg_count       BIGINT;
    v_dup_count       BIGINT;
    v_null_count      BIGINT;
    v_orphan_ticket_count BIGINT;
    v_orphan_flight_count BIGINT;
BEGIN
    -- Опорная метка: максимум src_created_at_ts среди предыдущих батчей
    SELECT max(src_created_at_ts)
    INTO v_prev_ts
    FROM stg.segments
    WHERE batch_id <> v_batch_id
        OR batch_id IS NULL;

    -- Источник: считаем строки во внешней таблице, которые вошли в окно инкремента
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.segments_ext AS s
    JOIN stg.tickets_ext AS t ON s.ticket_no = t.ticket_no
    JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
    WHERE b.book_date > COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');

    IF v_src_count = 0 THEN
        -- Пустое окно инкремента допустимо: новых данных может не быть.
        -- В этом случае ожидаем, что в текущем batch_id тоже 0 строк.
        SELECT COUNT(*)
        INTO v_stg_count
        FROM stg.segments
        WHERE batch_id = v_batch_id;

        IF v_stg_count <> 0 THEN
            RAISE EXCEPTION
                'DQ FAILED: источник segments_ext за окно инкремента пустой, но в stg.segments есть строки текущего batch_id (batch_id=%): %',
                v_batch_id,
                v_stg_count;
        END IF;

        RAISE NOTICE
            'В источнике segments_ext нет строк для окна инкремента (book_date > %). Пропускаем DQ проверки (batch_id=%).',
            COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00'),
            v_batch_id;
        RETURN;
    END IF;

    -- Считаем строки, реально вставленные в stg.segments в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.segments
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты (ticket_no, flight_id)
    SELECT COUNT(*) - COUNT(DISTINCT ticket_no || '|' || flight_id)
    INTO v_dup_count
    FROM stg.segments AS s
    WHERE s.batch_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты (ticket_no, flight_id) (batch_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей (ticket_no, flight_id, fare_conditions, price)
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.segments AS s
    WHERE s.batch_id = v_batch_id
        AND (s.ticket_no IS NULL OR s.ticket_no = ''
             OR s.flight_id IS NULL OR s.flight_id = ''
             OR s.fare_conditions IS NULL OR s.fare_conditions = ''
             OR s.price IS NULL OR s.price = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    -- Проверка ссылочной целостности: все segments должны иметь соответствующие tickets
    SELECT COUNT(*)
    INTO v_orphan_ticket_count
    FROM stg.segments AS s
    LEFT JOIN stg.tickets AS t ON s.ticket_no = t.ticket_no
    WHERE s.batch_id = v_batch_id
        AND t.ticket_no IS NULL;

    IF v_orphan_ticket_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены segments без соответствующих tickets (batch_id=%): %',
            v_batch_id,
            v_orphan_ticket_count;
    END IF;

    -- Проверка ссылочной целостности: все segments должны иметь соответствующие flights
    SELECT COUNT(*)
    INTO v_orphan_flight_count
    FROM stg.segments AS s
    LEFT JOIN stg.flights AS f ON s.flight_id = f.flight_id
    WHERE s.batch_id = v_batch_id
        AND f.flight_id IS NULL;

    IF v_orphan_flight_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены segments без соответствующих flights (batch_id=%): %',
            v_batch_id,
            v_orphan_flight_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: segments ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;
END $$;
