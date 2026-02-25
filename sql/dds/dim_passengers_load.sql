-- Загрузка DDS dim_passengers: SCD1 UPSERT (UPDATE изменившихся + INSERT новых).

-- Statement 1: UPDATE существующих записей (если атрибуты изменились).
WITH src AS (
    SELECT
        d.passenger_id,
        d.passenger_name
    FROM (
        SELECT
            t.passenger_id,
            t.passenger_name,
            ROW_NUMBER() OVER (
                PARTITION BY t.passenger_id
                ORDER BY t.event_ts DESC NULLS LAST, t._load_ts DESC, t.ticket_no DESC
            ) AS rn
        FROM ods.tickets AS t
        WHERE t.passenger_id IS NOT NULL
            AND t.passenger_id <> ''
            AND t.passenger_name IS NOT NULL
            AND t.passenger_name <> ''
    ) AS d
    WHERE d.rn = 1
)
UPDATE dds.dim_passengers AS d
SET passenger_name = s.passenger_name,
    updated_at     = now(),
    _load_id       = '{{ run_id }}',
    _load_ts       = now()
FROM src AS s
WHERE d.passenger_bk = s.passenger_id
    AND d.passenger_name IS DISTINCT FROM s.passenger_name;

-- Statement 2: INSERT новых записей (MAX(sk) + ROW_NUMBER()).
WITH src AS (
    SELECT
        d.passenger_id,
        d.passenger_name
    FROM (
        SELECT
            t.passenger_id,
            t.passenger_name,
            ROW_NUMBER() OVER (
                PARTITION BY t.passenger_id
                ORDER BY t.event_ts DESC NULLS LAST, t._load_ts DESC, t.ticket_no DESC
            ) AS rn
        FROM ods.tickets AS t
        WHERE t.passenger_id IS NOT NULL
            AND t.passenger_id <> ''
            AND t.passenger_name IS NOT NULL
            AND t.passenger_name <> ''
    ) AS d
    WHERE d.rn = 1
),
max_sk AS (
    SELECT COALESCE(MAX(passenger_sk), 0) AS v
    FROM dds.dim_passengers
)
INSERT INTO dds.dim_passengers (
    passenger_sk,
    passenger_bk,
    passenger_name,
    created_at,
    updated_at,
    _load_id,
    _load_ts
)
SELECT
    (SELECT v FROM max_sk) + ROW_NUMBER() OVER (ORDER BY s.passenger_id)::INTEGER,
    s.passenger_id,
    s.passenger_name,
    now(),
    now(),
    '{{ run_id }}',
    now()
FROM src AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.dim_passengers AS d
    WHERE d.passenger_bk = s.passenger_id
);

ANALYZE dds.dim_passengers;
