-- Проверки качества данных для boarding_passes

DO $$
DECLARE
    v_batch_id        TEXT      := '{{ run_id }}'::text;
    v_src_count       BIGINT;
    v_stg_count       BIGINT;
    v_dup_count       BIGINT;
    v_null_count      BIGINT;
    v_orphan_ticket_count BIGINT;
    v_orphan_segment_count BIGINT;
BEGIN
    -- Источник: считаем все строки во внешней таблице
    SELECT COUNT(*)
    INTO v_src_count
    FROM stg.boarding_passes_ext;

    IF v_src_count = 0 THEN
        RAISE NOTICE
            'В источнике boarding_passes_ext нет строк - пропускаем DQ проверки (batch_id=%).',
            v_batch_id;
        RETURN;
    END IF;

    -- Считаем строки, реально вставленные в stg.boarding_passes в этом батче
    SELECT COUNT(*)
    INTO v_stg_count
    FROM stg.boarding_passes
    WHERE batch_id = v_batch_id;

    IF v_src_count <> v_stg_count THEN
        RAISE EXCEPTION
            'DQ FAILED: несовпадение количества строк. Источник: %, STG: %',
            v_src_count,
            v_stg_count;
    END IF;

    -- Проверка на дубликаты (ticket_no, flight_id)
    -- Используем md5 от ROW, чтобы избежать коллизий при склейке строк.
    SELECT COUNT(*) - COUNT(DISTINCT md5(ROW(ticket_no, flight_id)::text))
    INTO v_dup_count
    FROM stg.boarding_passes AS bp
    WHERE bp.batch_id = v_batch_id;

    IF v_dup_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены дубликаты (ticket_no, flight_id) (batch_id=%): %',
            v_batch_id,
            v_dup_count;
    END IF;

    -- Проверка обязательных полей (ticket_no, flight_id)
    SELECT COUNT(*)
    INTO v_null_count
    FROM stg.boarding_passes AS bp
    WHERE bp.batch_id = v_batch_id
        AND (bp.ticket_no IS NULL OR bp.ticket_no = ''
             OR bp.flight_id IS NULL OR bp.flight_id = '');

    IF v_null_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены строки с NULL в обязательных полях (batch_id=%): %',
            v_batch_id,
            v_null_count;
    END IF;

    -- Проверка ссылочной целостности: все boarding_passes должны иметь соответствующие tickets
    SELECT COUNT(*)
    INTO v_orphan_ticket_count
    FROM stg.boarding_passes AS bp
    LEFT JOIN stg.tickets AS t ON bp.ticket_no = t.ticket_no
    WHERE bp.batch_id = v_batch_id
        AND t.ticket_no IS NULL;

    IF v_orphan_ticket_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены boarding_passes без соответствующих tickets (batch_id=%): %',
            v_batch_id,
            v_orphan_ticket_count;
    END IF;

    -- Проверка ссылочной целостности: все boarding_passes должны иметь соответствующие segments
    SELECT COUNT(*)
    INTO v_orphan_segment_count
    FROM stg.boarding_passes AS bp
    LEFT JOIN stg.segments AS s ON bp.ticket_no = s.ticket_no AND bp.flight_id = s.flight_id
    WHERE bp.batch_id = v_batch_id
        AND s.ticket_no IS NULL;

    IF v_orphan_segment_count <> 0 THEN
        RAISE EXCEPTION
            'DQ FAILED: найдены boarding_passes без соответствующих segments (batch_id=%): %',
            v_batch_id,
            v_orphan_segment_count;
    END IF;

    RAISE NOTICE
        'DQ PASSED: boarding_passes ок (batch_id=%): source=% stg=%',
        v_batch_id,
        v_src_count,
        v_stg_count;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'DQ ERROR для boarding_passes (batch_id=%): %', v_batch_id, SQLERRM;
    RAISE;
END $$;
