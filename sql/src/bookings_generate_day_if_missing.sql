-- Генерация учебного дня в демо-БД bookings.
-- Этот скрипт всегда добавляет следующий учебный день после максимальной даты
-- в таблице bookings.bookings (или несколько дней от стартовой даты, если база пуста).
-- Логическая дата запуска DAG ({{ ds }}) используется только как метка батча в стейдже.

DO $$
DECLARE
    v_max_book_date timestamptz;
    v_start_date    timestamptz;
    v_end_date      timestamptz;
    v_jobs          integer := COALESCE(current_setting('bookings.jobs', true), '1')::integer;
    v_init_days     integer := COALESCE(current_setting('bookings.init_days', true), '1')::integer;
    v_start_cfg     text    := COALESCE(current_setting('bookings.start_date', true), '2017-01-01');
BEGIN
    -- Проверяем, что демобаза установлена
    IF to_regclass('bookings.bookings') IS NULL THEN
        RAISE EXCEPTION 'Таблица bookings.bookings не найдена. Сначала выполните make bookings-init или make bookings-generate.';
    END IF;

    IF v_jobs < 1 THEN
        RAISE EXCEPTION 'bookings.jobs должен быть >= 1. Текущее значение: %.', v_jobs;
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

        -- Убираем VACUUM-ивенты из очереди: генератор demodb кладёт
        -- VACUUM ANALYZE всей БД каждую неделю модельного времени.
        -- На 500k+ строках это занимает минуты и бессмысленно для +1 дня.
        DELETE FROM gen.events WHERE type = 'VACUUM';

        CALL continue(v_end_date, v_jobs);
    END IF;

    -- continue() делает TRUNCATE gen.stat_jobs → AccessExclusiveLock.
    -- Без COMMIT воркеры не могут INSERT INTO gen.stat_jobs → deadlock.
    COMMIT;

    RAISE NOTICE 'Сгенерированы данные в bookings.bookings за интервал [% - %).',
        date_trunc('day', v_start_date),
        date_trunc('day', v_end_date);

    -- Ждём завершения каждого воркера через dblink_is_busy() (см. комментарий
    -- в bookings/generate_next_day.sql — busy() через pg_stat_activity ненадёжен).
    IF v_jobs > 1 THEN
        FOR i IN 1 .. v_jobs LOOP
            WHILE dblink_is_busy('job' || i) = 1 LOOP
                PERFORM pg_sleep(1);
            END LOOP;
        END LOOP;
    END IF;
    PERFORM dblink_disconnect(unnest(dblink_get_connections()));
END $$;
