-- DDL для базовой таблицы orders, которую использует CSV‑pipeline.
-- Выполняется идемпотентно: таблица создаётся, если ещё не существует.

CREATE TABLE IF NOT EXISTS public.orders (
    order_id    BIGINT,
    order_ts    TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1)
DISTRIBUTED BY (order_id);
