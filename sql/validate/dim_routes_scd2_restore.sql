-- Откат данных после активного теста SCD2.
-- Выполняется ВСЕГДА (trigger_rule="all_done"), даже если check упал.
--
-- Безопасность: если backup-шаг не создал таблицы (сбой на backup),
-- откат НЕ трогает live-данные — просто чистит служебные таблицы.
-- Это гарантирует, что restore никогда не сломает ods.routes / dds.dim_routes.
--
-- Паттерн: setup → act → assert → teardown (стандарт интеграционных тестов).

DO $$
DECLARE
    v_has_ods_backup BOOLEAN;
    v_has_dim_backup BOOLEAN;
BEGIN
    -- Проверяем существование backup-таблиц через to_regclass
    -- (ищет по search_path — совпадает с тем, как CREATE TABLE их создал)
    v_has_ods_backup := to_regclass('_validate_bk_ods_routes') IS NOT NULL;
    v_has_dim_backup := to_regclass('_validate_bk_dim_routes') IS NOT NULL;

    -- Восстанавливаем ods.routes только если бэкап существует
    IF v_has_ods_backup THEN
        TRUNCATE ods.routes;
        INSERT INTO ods.routes SELECT * FROM _validate_bk_ods_routes;
        RAISE NOTICE 'RESTORE: ods.routes восстановлена из бэкапа';
    ELSE
        RAISE NOTICE 'RESTORE: бэкап ods.routes не найден — пропускаем (backup-шаг не завершился?)';
    END IF;

    -- Восстанавливаем dds.dim_routes только если бэкап существует
    IF v_has_dim_backup THEN
        TRUNCATE dds.dim_routes;
        INSERT INTO dds.dim_routes SELECT * FROM _validate_bk_dim_routes;
        RAISE NOTICE 'RESTORE: dds.dim_routes восстановлена из бэкапа';
    ELSE
        RAISE NOTICE 'RESTORE: бэкап dds.dim_routes не найден — пропускаем';
    END IF;
END $$;

-- Cleanup служебных таблиц (безусловно, IF EXISTS)
DROP TABLE IF EXISTS _validate_bk_ods_routes;
DROP TABLE IF EXISTS _validate_bk_dim_routes;
DROP TABLE IF EXISTS _validate_scd2_target;
