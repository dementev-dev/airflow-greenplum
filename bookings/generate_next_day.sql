-- Если база пуста, берём стартовую дату из параметра :start_date (формат YYYY-MM-DD).
-- psql не умеет подставлять :start_date внутрь DO $$...$$, поэтому прокидываем дату через GUC.
SET bookings.start_date = :'start_date';
DO $$
DECLARE
    v_max_book_date timestamptz;
    v_start_date    timestamptz;
    v_end_date      timestamptz;
BEGIN
    -- Проверяем, что демобаза установлена
    IF to_regclass('bookings.bookings') IS NULL THEN
        RAISE EXCEPTION 'Таблица bookings.bookings не найдена. Сначала выполните make bookings-init.';
    END IF;

    -- Ищем последнюю сгенерированную дату
    SELECT max(book_date) INTO v_max_book_date FROM bookings.bookings;

    IF v_max_book_date IS NULL THEN
        -- База пустая: берём стартовую дату из переданного параметра
        v_start_date := date_trunc('day', current_setting('bookings.start_date')::timestamptz);
    ELSE
        -- Продолжаем с дня, следующего за максимальной датой
        v_start_date := date_trunc('day', v_max_book_date) + interval '1 day';
    END IF;

    v_end_date := v_start_date + interval '1 day';

    -- Первая генерация вызывает generate(), последующие — continue()
    IF v_max_book_date IS NULL THEN
        CALL generate(v_start_date, v_end_date, :'jobs');
    ELSE
        CALL continue(v_end_date, :'jobs');
    END IF;

    -- Ждём завершения фоновых джобов генератора, чтобы данные успели записаться
    WHILE busy() LOOP
        PERFORM pg_sleep(1);
    END LOOP;
    PERFORM dblink_disconnect(unnest(dblink_get_connections()));

END $$;
RESET bookings.start_date;
