# DAG `bookings_to_gp_dm`: `dds` -> `dm` в Greenplum

Этот DAG — учебный пример загрузки слоя **DM** (Data Mart / витрины) из текущего состояния **DDS**.
Логика: все 5 витрин загружаются параллельно, для каждой — пара `load -> dq`.

## Что делает DAG

- Загружает витрины DM параллельно (паттерны загрузки разные — учебная демонстрация выбора стратегии):
  - `dm.sales_report` — UPSERT по датам; DQ проверяет только строки текущего `run_id` (`_load_id`);
  - `dm.route_performance` — Full Rebuild (TRUNCATE + INSERT): таблица маленькая, дельту считать дороже;
  - `dm.passenger_loyalty` — инкрементальный UPSERT по «затронутым ключам» (HWM по `_load_ts`): пересчитываем агрегаты только для пассажиров с новыми фактами;
  - `dm.airport_traffic` — инкрементальный UPSERT по датам (HWM по `_load_ts`);
  - `dm.monthly_overview` — инкрементальный UPSERT по месяцам (HWM по `_load_ts`).
- Для каждой витрины выполняет пару задач `load -> dq`.

## Что должно быть готово перед запуском

1) Стенд поднят:

```bash
make up
```

2) STG, ODS и DDS уже загружены:

- выполнены DAG-и `bookings_to_gp_stage`, `bookings_to_gp_ods`, `bookings_to_gp_dds`;
- DDL-объекты созданы (`bookings_dm_ddl` или `make ddl-gp`).

## Как запустить

1) Откройте Airflow UI: http://localhost:8080.
2) Если запускаете DM впервые — выполните `bookings_dm_ddl`.
3) Запустите `bookings_to_gp_dm`.

## Граф зависимостей

Все 5 веток запускаются параллельно от `start_dm`, затем сходятся в `finish_dm_summary`:

```
start_dm
├── load_dm_sales_report      -> dq_dm_sales_report      -> finish_dm_summary
├── load_dm_route_performance -> dq_dm_route_performance -> finish_dm_summary
├── load_dm_passenger_loyalty -> dq_dm_passenger_loyalty -> finish_dm_summary
├── load_dm_airport_traffic   -> dq_dm_airport_traffic   -> finish_dm_summary
└── load_dm_monthly_overview  -> dq_dm_monthly_overview  -> finish_dm_summary
```

## Как проверить результат

```bash
make gp-psql
```

```sql
SELECT COUNT(*) FROM dm.sales_report;
SELECT COUNT(*) FROM dm.route_performance;
SELECT COUNT(*) FROM dm.passenger_loyalty;
SELECT COUNT(*) FROM dm.airport_traffic;
SELECT COUNT(*) FROM dm.monthly_overview;

-- Проверка инварианта sales_report: посаженных не больше, чем продано
SELECT COUNT(*) FROM dm.sales_report WHERE tickets_sold < passengers_boarded;

-- Проверка route_performance: нет дублей по бизнес-ключу
SELECT route_bk, COUNT(*) FROM dm.route_performance GROUP BY route_bk HAVING COUNT(*) > 1;
```

Ожидаемо: все витрины непусты, инварианты соблюдены, дублей нет.

## Типичные ошибки

- `relation "dm..." does not exist`:
  - не применён DM DDL (`bookings_dm_ddl` или `make ddl-gp`).
- DQ падает на `sales_report` по `boarding_rate`:
  - проверьте, что DDS загрузился корректно (`dds.fact_flight_sales` непуста).
- DQ падает на `passenger_loyalty` с ошибкой FK:
  - проверьте, что `dds.dim_passengers` содержит всех пассажиров из факта.
- `finish_dm_summary` не выполняется:
  - одна из DQ-задач упала; найдите в логах Airflow задачу с ошибкой и исправьте.
