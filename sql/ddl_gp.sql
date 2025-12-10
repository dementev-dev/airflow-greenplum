-- Главный входной DDL-скрипт для Greenplum в учебном стенде.
-- Выполняется из контейнера командой `make ddl-gp` и создаёт/обновляет
-- все объекты, которые нужны базовым DAG (csv_to_greenplum, bookings_to_gp_stage).
--
-- Идея такая:
--   - здесь описаны только верхнеуровневые объекты (orders, внешняя таблица bookings);
--   - более подробный DDL для отдельных слоёв (stg, src и т.п.) лежит в соседних файлах
--     в каталоге sql/ и подключается через psql-команду \i;
--   - чтобы не ломать задания, новые объекты лучше добавлять в отдельные файлы и
--     подключать их отсюда, а существующие определения не удалять.
--
-- Подробнее про STG/bookings: см. docs/internal/bookings_stg_readme.md.

-- Таблица для CSV‑пайплайна (csv_to_greenplum).
-- Колонночная таблица (append-optimized) и распределение по ключу.
-- Внимание: append-optimized таблицы не поддерживают UNIQUE/PRIMARY KEY,
-- поэтому контроль дублей выполняем в DAG при загрузке.
CREATE TABLE IF NOT EXISTS public.orders (
    order_id    BIGINT,
    order_ts    TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
DISTRIBUTED BY (order_id);

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

-- DDL для слоя stg по таблице bookings вынесен в отдельный файл.
-- Здесь подключаем его через psql \i, чтобы сохранить единый входной скрипт.
\i stg/bookings_ddl.sql
