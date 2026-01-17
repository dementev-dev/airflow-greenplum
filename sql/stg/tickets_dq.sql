-- Проверки качества данных для tickets

DO $$
DECLARE
    v_batch_id      TEXT      := '{{ run_id }}'::text;
    v_prev_ts       TIMESTAMP;
    v_source_count  BIGINT;
    v_stg_count     BIGINT;
    v_orphan_count  BIGINT;
    v_null_count    BIGINT;
BEGIN
    -- Опорная метка: максимум src_created_at_ts среди предыдущих батчей
    SELECT max(src_created_at_ts)
    INTO v_prev_ts
    FROM stg.tickets
    WHERE batch_id <> v_batch_id
        OR batch_id IS NULL;

    -- Количество в источнике (новые билеты в том же окне инкремента, что и загрузка)
    SELECT COUNT(*)
    INTO v_source_count
    FROM stg.tickets_ext AS t
    JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
    WHERE b.book_date > COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');

    IF v_source_count = 0 THEN
        RAISE EXCEPTION
            'В источнике tickets_ext нет строк для окна инкремента (book_date > %). Проверьте генерацию данных (таск generate_bookings_day).',
            COALESCE(v_prev_ts, TIMESTAMP '1900-01-01 00:00:00');
    END IF;

    -- Количество в STG (текущий батч)
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.tickets
    WHERE batch_id = v_batch_id;

    IF v_source_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества билетов. Источник: %, STG (batch_id=%): %',
            v_source_count,
            v_batch_id,
            v_stg_count;
    END IF;

    -- Проверка ссылочной целостности: все tickets должны иметь соответствующие bookings в этом же STG
    SELECT COUNT(*)
    INTO v_orphan_count
    FROM stg.tickets AS t
    WHERE t.batch_id = v_batch_id
        AND NOT EXISTS (
            SELECT 1
            FROM stg.bookings AS b
            WHERE b.book_ref = t.book_ref
        );

    IF v_orphan_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены tickets без соответствующих bookings (batch_id=%): %',
            v_batch_id,
            v_orphan_count;
    END IF;

    -- Проверка обязательных полей
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.tickets AS t
    WHERE t.batch_id = v_batch_id
        AND (t.ticket_no IS NULL OR t.book_ref IS NULL);

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены tickets с NULL в обязательных полях (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: tickets ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_source_count,
        v_stg_count;
END $$;
