-- DDL для DDS-слоя по таблице fact_flight_sales.

CREATE SCHEMA IF NOT EXISTS dds;

-- Учебный комментарий: Почему у факта нет своего суррогатного ключа (fact_sk)?
-- В классическом DWH (Кимбалл) таблица фактов идентифицируется набором её 
-- измерений или дегенеративных ключей (в нашем случае: ticket_no + flight_id).
-- Добавление отдельного ID только тратит место и не несёт аналитической ценности.
CREATE TABLE IF NOT EXISTS dds.fact_flight_sales (
    calendar_sk          INTEGER,
    departure_airport_sk INTEGER,
    arrival_airport_sk   INTEGER,
    airplane_sk          INTEGER,
    tariff_sk            INTEGER,
    passenger_sk         INTEGER,
    route_sk             INTEGER,
    book_ref             TEXT NOT NULL,
    ticket_no            TEXT NOT NULL,
    flight_id            INTEGER NOT NULL,
    book_date            DATE,
    seat_no              TEXT,
    price                NUMERIC(10,2),
    is_boarded           BOOLEAN NOT NULL,
    _load_id             TEXT NOT NULL,
    _load_ts             TIMESTAMP NOT NULL DEFAULT now()
)
DISTRIBUTED BY (ticket_no);
