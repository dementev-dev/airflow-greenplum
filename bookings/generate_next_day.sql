-- Генерация ещё одного дня данных в демобазе bookings.
-- Если база пуста, берём стартовую дату из параметра :start_date (формат YYYY-MM-DD).
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
        -- База пустая: берём стартовую дату из psql-переменной
        v_start_date := date_trunc('day', :'start_date'::timestamptz);
    ELSE
        -- Продолжаем с дня, следующего за максимальной датой
        v_start_date := date_trunc('day', v_max_book_date) + interval '1 day';
    END IF;

    v_end_date := v_start_date + interval '1 day';

    -- Первая генерация вызывает generate(), последующие — continue()
    IF v_max_book_date IS NULL THEN
        CALL generate(v_start_date, v_end_date, 1);
    ELSE
        CALL continue(v_end_date, 1);
    END IF;
END $$;

