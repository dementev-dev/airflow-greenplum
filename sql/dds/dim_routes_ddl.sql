-- DDL для DDS-слоя по таблице dim_routes (SCD2-измерение).

CREATE SCHEMA IF NOT EXISTS dds;

-- Тип таблицы: Heap (стандартная).
-- Обоснование: Необходим row-level UPDATE для реализации SCD2 (закрытие версий).
-- Использование Append-Only при частых обновлениях приводит к раздуванию (bloat) таблицы.
--
-- Учебный комментарий (Kimball Star Schema):
-- Измерение должно быть «самодостаточным»: один JOIN к dim_routes —
-- и аналитик видит маршрут, города, модель самолёта и кол-во мест.
-- Денормализованные атрибуты (departure_city, arrival_city, airplane_model, total_seats)
-- НЕ участвуют в hashdiff. Версия SCD2 фиксирует изменения атрибутов маршрута
-- (аэропорт, самолёт, расписание). Если изменится название города —
-- обновим отдельным refresh-шагом, не создавая новую версию.
CREATE TABLE IF NOT EXISTS dds.dim_routes (
    route_sk          INTEGER NOT NULL,
    route_bk          TEXT NOT NULL,
    departure_airport TEXT NOT NULL,
    arrival_airport   TEXT NOT NULL,
    airplane_code     TEXT NOT NULL,
    departure_city    TEXT NOT NULL,
    arrival_city      TEXT NOT NULL,
    airplane_model    TEXT NOT NULL,
    total_seats       INTEGER NOT NULL,
    days_of_week      TEXT,
    departure_time    TIME,
    duration          INTERVAL,
    hashdiff          TEXT NOT NULL,
    valid_from        DATE NOT NULL,
    valid_to          DATE,
    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    updated_at        TIMESTAMP NOT NULL DEFAULT now(),
    _load_id          TEXT NOT NULL,
    _load_ts          TIMESTAMP NOT NULL DEFAULT now()
)
WITH (appendonly=false)
DISTRIBUTED BY (route_sk);

COMMENT ON TABLE dds.dim_routes IS 'Измерение маршрутов (DDS).';
