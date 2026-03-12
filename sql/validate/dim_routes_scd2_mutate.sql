-- Мутация: сдвигаем departure_time у одного маршрута на 1 час.
-- Это должно изменить hashdiff → SCD2 должен закрыть старую версию и создать новую.
--
-- Сохраняем route_no тестового маршрута в служебную таблицу _validate_scd2_target,
-- чтобы check-скрипт точно знал, какой маршрут проверять (а не угадывал по побочным эффектам).
DO $$
DECLARE
    v_route TEXT;
    v_old_time TIME;
BEGIN
    -- Берём первый маршрут, у которого departure_time заполнен
    SELECT route_no, departure_time
    INTO v_route, v_old_time
    FROM ods.routes
    WHERE departure_time IS NOT NULL
    ORDER BY route_no
    LIMIT 1;

    IF v_route IS NULL THEN
        RAISE EXCEPTION 'FAILED: ods.routes пуста или нет маршрутов с departure_time. Загрузите STG→ODS перед проверкой.';
    END IF;

    -- Запоминаем тестовый маршрут в служебную таблицу
    DROP TABLE IF EXISTS _validate_scd2_target;
    CREATE TABLE _validate_scd2_target AS
    SELECT v_route AS route_no;

    -- Сдвигаем время на 1 час у всех записей этого маршрута
    UPDATE ods.routes
    SET departure_time = departure_time + INTERVAL '1 hour'
    WHERE route_no = v_route;

    RAISE NOTICE 'SCD2 TEST: маршрут % — departure_time сдвинут с % на %',
        v_route, v_old_time, v_old_time + INTERVAL '1 hour';
END $$;
