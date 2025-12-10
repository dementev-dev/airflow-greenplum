-- Генерация учебного дня в демо-БД bookings.
-- Если данные за указанный день уже существуют, генерация не выполняется.

DO $$
DECLARE
    v_has_day      boolean;
    v_load_date    date := {{ params.load_date }}::date;
    v_max_book_date timestamptz;
    v_start_date    timestamptz;
    v_end_date      timestamptz;
    v_jobs          integer := COALESCE(current_setting('bookings.jobs', true), '1')::integer;
    v_init_days     integer := COALESCE(current_setting('bookings.init_days', true), '1')::integer;
    v_start_cfg     text    := COALESCE(current_setting('bookings.start_date', true), '2017-01-01');
BEGIN
    -- Проверяем, что демобаза установлена
    IF to_regclass('bookings.bookings') IS NULL THEN
        RAISE EXCEPTION 'Таблица bookings.bookings не найдена. Сначала выполните make bookings-init.';
    END IF;

    -- Проверяем, есть ли уже данные за нужный день
    SELECT EXISTS (
        SELECT 1
        FROM bookings.bookings
        WHERE book_date::date = v_load_date
    )
    INTO v_has_day;

    IF v_has_day THEN
        RAISE NOTICE 'Данные за % уже есть в bookings.bookings — генерация не требуется.', v_load_date;
        RETURN;
    END IF;

    -- Ищем последнюю сгенерированную дату
    SELECT max(book_date) INTO v_max_book_date FROM bookings.bookings;

    IF v_max_book_date IS NULL THEN
        -- База пустая: берём стартовую дату из конфигурации (или дефолтную)
        v_start_date := date_trunc('day', v_start_cfg::timestamptz);
    ELSE
        -- Продолжаем с дня, следующего за максимальной датой
        v_start_date := date_trunc('day', v_max_book_date) + interval '1 day';
    END IF;

    -- Первая генерация вызывает generate(), последующие — continue()
    IF v_max_book_date IS NULL THEN
        v_end_date := v_start_date + (v_init_days || ' days')::interval;
        CALL generate(v_start_date, v_end_date, v_jobs);
    ELSE
        v_end_date := v_start_date + interval '1 day';
        CALL continue(v_end_date, v_jobs);
    END IF;

    -- Ждём завершения фоновых джобов генератора, чтобы данные успели записаться
    WHILE busy() LOOP
        PERFORM pg_sleep(1);
    END LOOP;
    PERFORM dblink_disconnect(unnest(dblink_get_connections()));
END $$;

