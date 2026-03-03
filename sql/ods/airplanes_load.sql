-- Загрузка ODS по airplanes: Полная перезагрузка (TRUNCATE + INSERT).
-- Почему: для справочников-снимков в Greenplum на AO-таблицах 
-- эффективнее перетереть данные целиком, чем делать медленный UPDATE.

TRUNCATE TABLE ods.airplanes;

INSERT INTO ods.airplanes (
    airplane_code,
    model,
    range_km,
    speed_kmh,
    _load_id,
    _load_ts
)
WITH src AS (
    -- Выбираем последний снимок из STG для текущего батча
    SELECT
        s.airplane_code,
        s.model::json->>'ru' AS model,
        NULLIF(s.range, '')::INTEGER AS range_km,
        NULLIF(s.speed, '')::INTEGER AS speed_kmh,
        ROW_NUMBER() OVER (
            PARTITION BY s.airplane_code
            ORDER BY s.load_dttm DESC, s.src_created_at_ts DESC NULLS LAST
        ) AS rn
    FROM stg.airplanes AS s
    WHERE s.batch_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
)
SELECT
    s.airplane_code,
    s.model,
    s.range_km,
    s.speed_kmh,
    '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text,
    now()
FROM src AS s
WHERE s.rn = 1;

ANALYZE ods.airplanes;
