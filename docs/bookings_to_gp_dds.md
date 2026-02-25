# DAG `bookings_to_gp_dds`: `ods` -> `dds` в Greenplum

Этот DAG — учебный пример загрузки аналитического слоя **DDS** (Star Schema) из текущего состояния **ODS**.
Логика: измерения + факт, проверки качества данных после каждой загрузки.

## Что делает DAG

- Загружает измерения DDS:
  - `dds.dim_calendar` (статическое измерение дат);
  - `dds.dim_airports`, `dds.dim_airplanes`, `dds.dim_tariffs`, `dds.dim_passengers` (SCD1 UPSERT);
  - `dds.dim_routes` (SCD2 с `hashdiff`, `valid_from`, `valid_to`).
- Загружает факт `dds.fact_flight_sales` инкрементальным UPSERT по зерну `(ticket_no, flight_id)`.
- Для каждой таблицы выполняет пару задач `load -> dq`.
- Использует `_load_id = {{ run_id }}` (DDS не требует `stg_batch_id`, потому что читает current state ODS).

## Что должно быть готово перед запуском

1) Стенд поднят:

```bash
make up
```

2) STG и ODS уже загружены:

- выполнены DAG-и `bookings_to_gp_stage` и `bookings_to_gp_ods`;
- DDL-объекты созданы (`bookings_dds_ddl` или `make ddl-gp`).

## Как запустить

1) Откройте Airflow UI: http://localhost:8080.
2) Если запускаете DDS впервые — выполните `bookings_dds_ddl`.
3) Запустите `bookings_to_gp_dds`.

## Граф зависимостей (упрощённо)

- `load_dds_dim_calendar -> dq_dds_dim_calendar`
- После calendar параллельно:
  - `load_dds_dim_airports -> dq_dds_dim_airports`
  - `load_dds_dim_airplanes -> dq_dds_dim_airplanes`
  - `load_dds_dim_tariffs -> dq_dds_dim_tariffs`
  - `load_dds_dim_passengers -> dq_dds_dim_passengers`
  - `load_dds_dim_routes -> dq_dds_dim_routes`
- Факт:
  - `load_dds_fact_flight_sales -> dq_dds_fact_flight_sales -> finish_dds_summary`

## Как проверить результат

```bash
make gp-psql
```

```sql
SELECT COUNT(*) FROM dds.dim_calendar;
SELECT COUNT(*) FROM dds.dim_routes;
SELECT COUNT(*) FROM dds.fact_flight_sales;

SELECT
    (SELECT COUNT(*) FROM dds.fact_flight_sales) AS fact_rows,
    (SELECT COUNT(*) FROM ods.segments) AS ods_rows;
```

Ожидаемо: `fact_rows = ods_rows`.

## Типичные ошибки

- `relation "dds..." does not exist`:
  - не применён DDS DDL (`bookings_dds_ddl` или `make ddl-gp`).
- DQ падает на `dim_routes`:
  - проверьте согласованность `ods.routes` (дубли/аномальные версии) и перезапустите DAG.
- DQ падает на `fact_flight_sales` по coverage:
  - проверьте, что ODS DAG завершился успешно без пропуска задач.
