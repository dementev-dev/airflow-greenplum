-- Проверки качества данных для tickets

-- Проверка 1: совпадение количества билетов в источнике и STG
DO $$
DECLARE
    v_source_count BIGINT;
    v_stg_count BIGINT;
BEGIN
    -- Количество в источнике (новые билеты)
    -- Используем только внешние таблицы: stg.tickets_ext + stg.bookings_ext
    SELECT COUNT(*) INTO v_source_count
    FROM stg.tickets_ext AS t
    JOIN stg.bookings_ext AS b ON t.book_ref = b.book_ref
    WHERE b.book_date > COALESCE(
        (SELECT max(src_created_at_ts) FROM stg.tickets
         WHERE batch_id <> '{{ run_id }}'::text OR batch_id IS NULL),
        TIMESTAMP '1900-01-01 00:00:00'
    );

    -- Количество в STG (текущий батч)
    SELECT COUNT(*) INTO v_stg_count
    FROM stg.tickets
    WHERE batch_id = '{{ run_id }}'::text;

    -- Проверка совпадения
    IF v_source_count <> v_stg_count THEN
        RAISE EXCEPTION 'DQ FAILED: несовпадение количества билетов. Источник: %, STG: %',
                          v_source_count, v_stg_count;
    ELSE
        RAISE NOTICE 'DQ PASSED: количество билетов совпадает (%)', v_stg_count;
    END IF;
END $$;

-- Проверка 2: ссылочная целостность (все ticket_no должны иметь соответствующие book_ref)
SELECT
    COUNT(*) AS orphan_tickets
FROM stg.tickets
WHERE batch_id = '{{ run_id }}'::text
    AND NOT EXISTS (
        SELECT 1
        FROM stg.bookings
        WHERE stg.bookings.book_ref = stg.tickets.book_ref
    );
-- Ожидаемое значение: 0

-- Проверка 3: отсутствие NULL в обязательных полях
SELECT
    COUNT(*) AS null_tickets
FROM stg.tickets
WHERE batch_id = '{{ run_id }}'::text
    AND (ticket_no IS NULL OR book_ref IS NULL);
-- Ожидаемое значение: 0
