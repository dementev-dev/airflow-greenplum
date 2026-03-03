-- Загрузка ODS по routes: Полная перезагрузка (TRUNCATE + INSERT).
-- Почему: для справочников-снимков в Greenplum на AO-таблицах 
-- эффективнее перетереть данные целиком, чем делать медленный UPDATE.

TRUNCATE TABLE ods.routes;

INSERT INTO ods.routes (
    route_no,
    validity,
    departure_airport,
    arrival_airport,
    airplane_code,
    days_of_week,
    departure_time,
    duration,
    _load_id,
    _load_ts
)
WITH src AS (
    -- Выбираем последний снимок из STG для текущего батча
    SELECT
        s.route_no,
        s.validity,
        s.departure_airport,
        s.arrival_airport,
        s.airplane_code,
        s.days_of_week::INTEGER[] AS days_of_week,
        NULLIF(s.scheduled_time, '')::TIME AS departure_time,
        NULLIF(s.duration, '')::INTERVAL   AS duration,
        ROW_NUMBER() OVER (
            PARTITION BY s.route_no, s.validity
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.routes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
SELECT
    s.route_no,
    s.validity,
    s.departure_airport,
    s.arrival_airport,
    s.airplane_code,
    s.days_of_week::INTEGER[],
    s.departure_time,
    s.duration,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1;

ANALYZE ods.routes;
