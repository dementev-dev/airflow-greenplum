-- DDL для витрины dm.passenger_loyalty (Лояльность пассажиров).
--
-- Учебные цели:
-- 1. Выбор зерна для SCD1-измерений:
--    В SCD2-витринах (напр. route_performance) зерно — бизнес-ключ (BK), т.к. версий много.
--    В SCD1 (dim_passengers) BK и SK связаны 1:1, поэтому зерном выступает суррогатный ключ (SK).
--    Это упрощает JOIN с фактами и ускоряет запросы в Greenplum.
-- 2. Использование Heap-таблицы для UPSERT:
--    В отличие от AO Column, Heap поддерживает эффективный UPDATE, что критично для
--    накопительных витрин с большим количеством строк.
-- 3. Служебные поля жизненного цикла:
--    Т.к. мы будем обновлять метрики пассажиров, нам нужны и created_at, и updated_at.

CREATE TABLE IF NOT EXISTS dm.passenger_loyalty (
    -- Ключ: суррогатный ключ пассажира (SCD1 гарантирует 1:1 к бизнес-ключу)
    passenger_sk           INTEGER NOT NULL,
    passenger_bk           TEXT NOT NULL,
    passenger_name         TEXT NOT NULL,

    -- Метрики лояльности
    total_bookings         INTEGER NOT NULL,     -- кол-во бронирований (book_ref)
    total_flights          INTEGER NOT NULL,     -- кол-во перелетов
    total_boarded          INTEGER NOT NULL,     -- кол-во успешных посадок
    total_spent            NUMERIC(15,2) NOT NULL,
    avg_ticket_price       NUMERIC(10,2),
    favorite_fare_conditions TEXT,               -- самый частый класс обслуживания
    unique_routes          INTEGER NOT NULL,     -- кол-во уникальных маршрутов
    first_flight_date      DATE,
    last_flight_date       DATE,
    days_as_customer       INTEGER,              -- стаж клиента (дней между первым и последним)

    -- Служебные поля
    created_at             TIMESTAMP NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP NOT NULL DEFAULT now(),
    _load_id               TEXT NOT NULL,
    _load_ts               TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false) -- Явно указываем Heap для поддержки эффективного UPDATE
DISTRIBUTED BY (passenger_sk);

COMMENT ON TABLE dm.passenger_loyalty IS 'Витрина: лояльность и активность пассажиров (Incremental UPSERT, Heap)';
