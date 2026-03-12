-- Загрузка DDS dim_calendar: статическое измерение (генерация дат).
-- Заполняем только если таблица пуста (идемпотентно).

INSERT INTO dds.dim_calendar (
    calendar_sk,
    date_actual,
    year_actual,
    month_actual,
    day_actual,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    ROW_NUMBER() OVER (ORDER BY d.date_actual)::INTEGER AS calendar_sk,
    d.date_actual,
    EXTRACT(YEAR FROM d.date_actual)::INTEGER    AS year_actual,
    EXTRACT(MONTH FROM d.date_actual)::INTEGER   AS month_actual,
    EXTRACT(DAY FROM d.date_actual)::INTEGER     AS day_actual,
    EXTRACT(ISODOW FROM d.date_actual)::INTEGER  AS day_of_week,
    TO_CHAR(d.date_actual, 'FMDay')              AS day_name,
    EXTRACT(ISODOW FROM d.date_actual) IN (6, 7) AS is_weekend
FROM (
    SELECT generate_series('2016-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL)::DATE
        AS date_actual
) AS d
WHERE NOT EXISTS (SELECT 1 FROM dds.dim_calendar LIMIT 1);

ANALYZE dds.dim_calendar;
