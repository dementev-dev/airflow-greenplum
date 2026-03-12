-- Проверяем, что SCD2-логика студента сработала корректно после мутации.
-- Читаем route_no тестового маршрута из служебной таблицы _validate_scd2_target
-- (создана на шаге mutate), а не угадываем по побочным эффектам.
DO $$
DECLARE
    v_route TEXT;
    v_version_count BIGINT;
    v_closed_count BIGINT;
    v_open_count BIGINT;
    v_old_hash TEXT;
    v_new_hash TEXT;
    v_gap_count BIGINT;
BEGIN
    -- Читаем тестовый маршрут из служебной таблицы
    SELECT route_no INTO v_route FROM _validate_scd2_target LIMIT 1;

    IF v_route IS NULL THEN
        RAISE EXCEPTION 'FAILED: служебная таблица _validate_scd2_target пуста. Шаг mutate не выполнился?';
    END IF;

    -- Если в dim_routes нет маршрута — load не запустился или упал
    IF NOT EXISTS (SELECT 1 FROM dds.dim_routes WHERE route_bk = v_route) THEN
        RAISE EXCEPTION E'FAILED: dds.dim_routes не содержит маршрут % после запуска load.\n'
            'Проверьте sql/dds/dim_routes_load.sql.', v_route;
    END IF;

    -- Проверка a: Ровно 2 версии тестового маршрута (было 1, стало 2 после мутации)
    SELECT COUNT(*) INTO v_version_count
    FROM dds.dim_routes WHERE route_bk = v_route;

    IF v_version_count < 2 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — найдена % версия (ожидается 2: старая закрытая + новая открытая).\n'
            'SCD2 должен был создать новую версию после изменения departure_time.', v_route, v_version_count;
    END IF;

    IF v_version_count > 2 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — найдено % версий (ожидается 2).\n'
            'Возможно, load создаёт лишние дубликаты. Проверьте условие NOT EXISTS при INSERT.', v_route, v_version_count;
    END IF;

    -- Проверка b: Старая версия закрыта (valid_to IS NOT NULL)
    SELECT COUNT(*) INTO v_closed_count
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NOT NULL;

    IF v_closed_count = 0 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — SCD2 не закрыл старую версию (valid_to IS NULL у всех версий).\n'
            'Подсказка: hashdiff изменился (departure_time сдвинут на 1 час),\n'
            'но ваш load-скрипт не обнаружил это изменение.\n'
            'Проверьте:\n'
            '  1. Формулу hashdiff — включает ли она departure_time?\n'
            '  2. Логику сравнения hashdiff (UPDATE ... SET valid_to = CURRENT_DATE WHERE hashdiff <> новый_hashdiff)', v_route;
    END IF;

    -- Проверка c: Новая версия открыта (valid_to IS NULL)
    SELECT COUNT(*) INTO v_open_count
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NULL;

    IF v_open_count <> 1 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — ожидается ровно 1 открытая версия (valid_to IS NULL), найдено %.\n'
            'Подсказка: SCD2 должен вставить новую строку с valid_to = NULL.', v_route, v_open_count;
    END IF;

    -- Проверка d: hashdiff старой ≠ hashdiff новой (мутация действительно отразилась в хеше)
    SELECT hashdiff INTO v_old_hash
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NOT NULL
    ORDER BY valid_from DESC LIMIT 1;

    SELECT hashdiff INTO v_new_hash
    FROM dds.dim_routes WHERE route_bk = v_route AND valid_to IS NULL;

    IF v_old_hash = v_new_hash THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — hashdiff старой и новой версий совпадают.\n'
            'Мутация сдвинула departure_time на 1 час, но hashdiff не изменился.\n'
            'Проверьте, что departure_time входит в формулу hashdiff.', v_route;
    END IF;

    -- Проверка e: Нет «дыры» между valid_to старой и valid_from новой
    SELECT COUNT(*) INTO v_gap_count
    FROM dds.dim_routes AS old_v
    JOIN dds.dim_routes AS new_v
        ON old_v.route_bk = new_v.route_bk
    WHERE old_v.route_bk = v_route
        AND old_v.valid_to IS NOT NULL
        AND new_v.valid_to IS NULL
        AND old_v.valid_to <> new_v.valid_from;

    IF v_gap_count > 0 THEN
        RAISE EXCEPTION E'FAILED: Маршрут % — «дыра» между версиями:\n'
            'valid_to старой ≠ valid_from новой.\n'
            'Подсказка: полуоткрытый интервал [valid_from, valid_to).\n'
            'valid_from новой версии должен = valid_to старой (обычно CURRENT_DATE).', v_route;
    END IF;

    RAISE NOTICE 'PASSED: SCD2 корректен для маршрута %. 2 версии, hashdiff различаются, «дыр» нет.', v_route;
END $$;
