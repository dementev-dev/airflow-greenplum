-- Загрузка ODS по seats: Полная перезагрузка (TRUNCATE + INSERT).
-- Почему: для справочников-снимков в Greenplum на AO-таблицах 
-- эффективнее перетереть данные целиком, чем делать медленный UPDATE.

TRUNCATE TABLE ods.seats;

INSERT INTO ods.seats (
    airplane_code,
    seat_no,
    fare_conditions,
    _load_id,
    _load_ts
)
WITH src AS (
    -- Выбираем последний снимок из STG для текущего батча
    SELECT
        s.airplane_code,
        s.seat_no,
        s.fare_conditions,
        ROW_NUMBER() OVER (
            PARTITION BY s.airplane_code, s.seat_no
            ORDER BY s._load_ts DESC, s.event_ts DESC NULLS LAST
        ) AS rn
    FROM stg.seats AS s
    WHERE s._load_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
SELECT
    s.airplane_code,
    s.seat_no,
    s.fare_conditions,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1;

ANALYZE ods.seats;
