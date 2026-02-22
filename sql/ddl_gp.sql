-- Главный входной DDL-скрипт для Greenplum в учебном стенде.
-- Выполняется из контейнера командой `make ddl-gp` и создаёт/обновляет
-- все объекты, которые нужны базовым DAG (csv_to_greenplum, bookings_to_gp_stage);
-- подключает файловые DDL через \i, чтобы сохранять единый входной скрипт.
--
-- Чтобы не ломать задания, новые объекты лучше добавлять в отдельные файлы
-- и подключать их отсюда, а существующие определения не удалять.
--
-- Подробнее про STG/bookings: см. docs/bookings_to_gp_stage.md.

-- Таблица для CSV‑пайплайна (csv_to_greenplum).
\i base/orders_ddl.sql

-- Внешняя таблица для чтения данных из демо-БД bookings через PXF (JDBC).
-- Источник: таблица bookings.bookings в базе demo (Postgres, сервис bookings-db).
DROP EXTERNAL TABLE IF EXISTS public.ext_bookings_bookings;
CREATE EXTERNAL TABLE public.ext_bookings_bookings (
    book_ref     CHAR(6),
    book_date    TIMESTAMP,
    total_amount NUMERIC(10,2)
)
LOCATION ('pxf://bookings.bookings?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

-- DDL для слоя stg по таблицам bookings и tickets вынесены в отдельные файлы.
-- Здесь подключаем их через psql \i, чтобы сохранить единый входной скрипт.
\i stg/bookings_ddl.sql
\i stg/tickets_ddl.sql

-- DDL для новых таблиц STG слоя (справочники)
\i stg/airports_ddl.sql
\i stg/airplanes_ddl.sql
\i stg/routes_ddl.sql
\i stg/seats_ddl.sql

-- DDL для новых таблиц STG слоя (транзакции)
\i stg/flights_ddl.sql
\i stg/segments_ddl.sql
\i stg/boarding_passes_ddl.sql

-- DDL для ODS-слоя (текущее состояние, SCD1).
\i ods/airports_ddl.sql
\i ods/airplanes_ddl.sql
\i ods/routes_ddl.sql
\i ods/seats_ddl.sql
\i ods/bookings_ddl.sql
\i ods/tickets_ddl.sql
\i ods/flights_ddl.sql
\i ods/segments_ddl.sql
\i ods/boarding_passes_ddl.sql
