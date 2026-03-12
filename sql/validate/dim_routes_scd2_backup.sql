-- Бэкап текущего состояния перед активным тестом SCD2.
-- Используем обычные таблицы (не TEMP) — между тасками Airflow
-- TEMP-таблицы не сохраняются (каждый таск = отдельная транзакция).

DROP TABLE IF EXISTS _validate_bk_ods_routes;
CREATE TABLE _validate_bk_ods_routes AS SELECT * FROM ods.routes;

DROP TABLE IF EXISTS _validate_bk_dim_routes;
CREATE TABLE _validate_bk_dim_routes AS SELECT * FROM dds.dim_routes;

-- Cleanup служебной таблицы от предыдущего запуска (на случай если restore не доехал)
DROP TABLE IF EXISTS _validate_scd2_target;
