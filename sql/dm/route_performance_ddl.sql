-- DDL для витрины dm.route_performance (Эффективность маршрутов).
--
-- Учебные цели:
-- 1. Демонстрация AO Column Store (Append-Only Columnar Storage).
--    В Greenplum формат AO Column идеален для аналитики (сжатие, чтение только нужных колонок).
-- 2. Демонстрация стратегии Full Rebuild (TRUNCATE + INSERT).
--    Для небольших справочных витрин это проще и надёжнее, чем сложный инкремент.
-- 3. Выбор служебных полей:
--    Для Full Rebuild таблиц created_at/updated_at не имеют смысла, т.к. строки
--    каждый раз пересоздаются. Достаточно _load_ts.

CREATE TABLE IF NOT EXISTS dm.route_performance (
    -- Ключ: бизнес-код маршрута (напр. 'SVO-LED')
    route_bk               TEXT NOT NULL,

    -- SK текущей (актуальной) версии маршрута для связи с измерениями
    route_sk               INTEGER NOT NULL,

    -- Денормализованные атрибуты (из актуальной версии dim_routes)
    departure_airport_bk   TEXT NOT NULL,
    departure_city         TEXT NOT NULL,
    arrival_airport_bk     TEXT NOT NULL,
    arrival_city           TEXT NOT NULL,
    airplane_bk            TEXT NOT NULL,
    airplane_model         TEXT NOT NULL,
    total_seats            INTEGER NOT NULL,

    -- Метрики (агрегированы по всем версиям данного маршрута)
    total_flights          INTEGER NOT NULL,
    total_tickets          INTEGER NOT NULL,
    total_boarded          INTEGER NOT NULL,
    total_revenue          NUMERIC(15,2) NOT NULL,
    avg_ticket_price       NUMERIC(10,2),
    avg_boarding_rate      NUMERIC(5,4) NOT NULL,
    avg_load_factor        NUMERIC(5,4),  -- средняя заполняемость кресел
    first_flight_date      DATE,
    last_flight_date       DATE,

    -- Служебные поля.
    -- created_at/updated_at здесь не нужны: при Full Rebuild все строки пересоздаются,
    -- поэтому _load_ts достаточно для отслеживания момента загрузки.
    _load_id               TEXT NOT NULL,
    _load_ts               TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (route_bk);

COMMENT ON TABLE dm.route_performance IS 'Витрина: эффективность авиамаршрутов (Full Rebuild, AO Column, zstd)';
