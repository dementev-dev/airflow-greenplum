-- Проверки качества данных для boarding_passes (инкрементальная загрузка).

DO $$
DECLARE
    v_batch_id             TEXT      := '{{ run_id }}'::text;
    v_prev_ts              TIMESTAMP;
    v_src_count            BIGINT;
    v_stg_count            BIGINT;
    v_dup_count            BIGINT;
    v_null_count           BIGINT;
    v_orphan_ticket_count  BIGINT;
    v_orphan_segment_count BIGINT;
BEGIN
    -- Опорная метка: максимум event_ts среди предыдущих батчей
    SELECT MAX(event_ts)
    INTO v_prev_ts
    FROM stg.boarding_passes
    WHERE _load_id <> v_batch_id
        OR _load_id IS NULL;

    -- Количество в источнике (boarding_passes в том же окне инкремента, что и загрузка)
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.boarding_passes_ext AS ext
    JOIN stg.tickets_ext AS t ON ext.ticket_no = t.ticket_no
    JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
    WHERE b.book_date > COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');

    IF v_src_count = 0 THEN
        -- Пустое окно инкремента допустимо: новых данных может не быть.
        SELECT COUNT(*)
        INTO v_stg_count
        FROM stg.boarding_passes
        WHERE _load_id = v_batch_id;

        IF v_stg_count <> 0 THEN
            RAISE EXCEPTION
                'DQ FAILED: источник boarding_passes_ext за окно инкремента пустой, но в stg.boarding_passes есть строки текущего _load_id (_load_id=%): %',
                v_batch_id,
                v_stg_count;
        END IF;

        RAISE NOTICE
            'В источнике boarding_passes_ext нет строк для окна инкремента (book_date > %). Пропускаем DQ проверки (_load_id=%).',
            COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00'),
            v_batch_id;
        RETURN;
    END IF;

    -- Считаем строки, реально вставленные в stg.boarding_passes в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.boarding_passes
    WHERE _load_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник (окно инкремента): %, STG (_load_id=%): %',
            v_src_count,
            v_batch_id,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты (ticket_no, flight_id) в текущем батче
    SELECT COUNT(*) - COUNT(DISTINCT md5(ROW(ticket_no, flight_id)::text))
    INTO v_dup_count
    FROM stg.boarding_passes AS bp
    WHERE bp._load_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты (ticket_no, flight_id) (_load_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей (ticket_no, flight_id)
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.boarding_passes AS bp
    WHERE bp._load_id = v_batch_id
        AND (bp.ticket_no IS NULL OR bp.ticket_no = ''
             OR bp.flight_id IS NULL OR bp.flight_id = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (_load_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    -- Проверка ссылочной целостности: все boarding_passes должны иметь соответствующие tickets
    SELECT COUNT(*)
    INTO v_orphan_ticket_count
    FROM stg.boarding_passes AS bp
    LEFT JOIN stg.tickets AS t ON bp.ticket_no = t.ticket_no
    WHERE bp._load_id = v_batch_id
        AND t.ticket_no IS NULL;

    IF v_orphan_ticket_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены boarding_passes без соответствующих tickets (_load_id=%): %',
            v_batch_id,
            v_orphan_ticket_count;
    END IF;

    -- Проверка ссылочной целостности: все boarding_passes должны иметь соответствующие segments
    SELECT COUNT(*)
    INTO v_orphan_segment_count
    FROM stg.boarding_passes AS bp
    LEFT JOIN stg.segments AS s ON bp.ticket_no = s.ticket_no AND bp.flight_id = s.flight_id
    WHERE bp._load_id = v_batch_id
        AND s.ticket_no IS NULL;

    IF v_orphan_segment_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены boarding_passes без соответствующих segments (_load_id=%): %',
            v_batch_id,
            v_orphan_segment_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: boarding_passes ок (_load_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'DQ ERROR для boarding_passes (_load_id=%): %', v_batch_id, SQLERRM;
    RAISE;
END $$;
