-- Загрузка DDS fact_flight_sales: инкрементальный UPSERT по (ticket_no, flight_id).

-- Statement 1: UPDATE существующих строк факта.
-- Обновляем только мутабельные поля; SK измерений не перезаписываем.
UPDATE dds.fact_flight_sales AS f
SET seat_no    = bp.seat_no,
    price      = seg.segment_amount,
    is_boarded = (bp.ticket_no IS NOT NULL),
    _load_id   = '{{ run_id }}',
    _load_ts   = now()
FROM ods.segments AS seg
LEFT JOIN ods.boarding_passes AS bp
    ON bp.ticket_no = seg.ticket_no
    AND bp.flight_id = seg.flight_id
WHERE f.ticket_no = seg.ticket_no
    AND f.flight_id = seg.flight_id
    AND (
        f.is_boarded IS DISTINCT FROM (bp.ticket_no IS NOT NULL)
        OR f.price IS DISTINCT FROM seg.segment_amount
        OR f.seat_no IS DISTINCT FROM bp.seat_no
    );

-- Statement 2: INSERT новых строк факта.
-- Dimension SK фиксируются на момент вставки (point-in-time для SCD2 routes).
WITH fact_src AS (
    SELECT
        seg.ticket_no,
        seg.flight_id,
        cal.calendar_sk,
        dep.airport_sk AS departure_airport_sk,
        arr.airport_sk AS arrival_airport_sk,
        ap.airplane_sk,
        tar.tariff_sk,
        pax.passenger_sk,
        rte.route_sk,
        tkt.book_ref,
        bkg.book_date::DATE AS book_date,
        bp.seat_no,
        seg.segment_amount AS price,
        (bp.ticket_no IS NOT NULL) AS is_boarded
    FROM ods.segments AS seg
    JOIN ods.tickets AS tkt
        ON tkt.ticket_no = seg.ticket_no
    JOIN ods.bookings AS bkg
        ON bkg.book_ref = tkt.book_ref
    JOIN ods.flights AS flt
        ON flt.flight_id = seg.flight_id
    LEFT JOIN dds.dim_routes AS rte
        ON rte.route_bk = flt.route_no
        AND flt.scheduled_departure::DATE >= rte.valid_from
        AND (rte.valid_to IS NULL OR flt.scheduled_departure::DATE < rte.valid_to)
    LEFT JOIN dds.dim_calendar AS cal
        ON cal.date_actual = flt.scheduled_departure::DATE
    LEFT JOIN dds.dim_airports AS dep
        ON dep.airport_bk = rte.departure_airport
    LEFT JOIN dds.dim_airports AS arr
        ON arr.airport_bk = rte.arrival_airport
    LEFT JOIN dds.dim_airplanes AS ap
        ON ap.airplane_bk = rte.airplane_code
    LEFT JOIN dds.dim_tariffs AS tar
        ON tar.fare_conditions = seg.fare_conditions
    LEFT JOIN dds.dim_passengers AS pax
        ON pax.passenger_bk = tkt.passenger_id
    LEFT JOIN ods.boarding_passes AS bp
        ON bp.ticket_no = seg.ticket_no
        AND bp.flight_id = seg.flight_id
)
INSERT INTO dds.fact_flight_sales (
    calendar_sk,
    departure_airport_sk,
    arrival_airport_sk,
    airplane_sk,
    tariff_sk,
    passenger_sk,
    route_sk,
    book_ref,
    ticket_no,
    flight_id,
    book_date,
    seat_no,
    price,
    is_boarded,
    _load_id,
    _load_ts
)
SELECT
    s.calendar_sk,
    s.departure_airport_sk,
    s.arrival_airport_sk,
    s.airplane_sk,
    s.tariff_sk,
    s.passenger_sk,
    s.route_sk,
    s.book_ref,
    s.ticket_no,
    s.flight_id,
    s.book_date,
    s.seat_no,
    s.price,
    s.is_boarded,
    '{{ run_id }}',
    now()
FROM fact_src AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.fact_flight_sales AS f
    WHERE f.ticket_no = s.ticket_no
        AND f.flight_id = s.flight_id
);

ANALYZE dds.fact_flight_sales;
