# Учебные задания

> Это ветка `solution` — здесь всё уже реализовано.
> Используйте её как справочник, а задания выполняйте на ветке `main`.

## Навигатор по реализации

На этой ветке полностью работает пайплайн `STG → ODS → DDS → DM`.
Ниже — рекомендуемый порядок изучения: от простых паттернов к сложным.

### ODS: загрузка из STG

| Паттерн | Файл | Что посмотреть |
|---------|------|----------------|
| TRUNCATE + INSERT (snapshot-справочник) | `sql/ods/airports_load.sql` | Фильтрация по `stg_batch_id`, дедупликация через `ROW_NUMBER()` |
| SCD1 UPSERT (транзакционные данные) | `sql/ods/bookings_load.sql` | TEMP TABLE → UPDATE (`IS DISTINCT FROM`) → INSERT, HWM |

### DDS: измерения и факт

| Паттерн | Файл | Что посмотреть |
|---------|------|----------------|
| SCD1 (простое измерение) | `sql/dds/dim_airports_load.sql` | UPSERT с суррогатным ключом |
| SCD1 с обогащением | `sql/dds/dim_airplanes_load.sql` | JOIN с `ods.seats` для `total_seats` |
| SCD2 (версионирование) | `sql/dds/dim_routes_load.sql` | `hashdiff`, `valid_from`/`valid_to`, денормализация |
| Факт с point-in-time JOIN | `sql/dds/fact_flight_sales_load.sql` | SCD2 lookup, защитные LEFT JOIN |

### DM: витрины

| Паттерн | Файл | Что посмотреть |
|---------|------|----------------|
| UPSERT с HWM по датам | `sql/dm/sales_report_load.sql` | Инкрементальная агрегация фактов |
| Full Rebuild (AO Column) | `sql/dm/route_performance_load.sql` | TRUNCATE + INSERT, агрегация по SCD2 `route_bk` |
| UNION ALL (dual-role dimension) | `sql/dm/airport_traffic_load.sql` | Разворот факта: departure + arrival |
| Двухуровневая агрегация | `sql/dm/monthly_overview_load.sql` | Честный `avg_load_factor` через 2 уровня |
| Метод затронутых ключей | `sql/dm/passenger_loyalty_load.sql` | HWM по пассажирам, пересчёт полной истории |

## Техническое задание

Основной документ: **[analyst_spec.md](analyst_spec.md)** — ТЗ от аналитика
с описанием всех таблиц, маппингами, бизнес-правилами и подсказками.

## Описания DAG-ов

- [STG](../bookings_to_gp_stage.md) ·
  [ODS](../bookings_to_gp_ods.md) ·
  [DDS](../bookings_to_gp_dds.md) ·
  [DM](../bookings_to_gp_dm.md)
- [Порядок запуска DAG-ов](../reference/dag_execution_order.md)

## Для менторов

Дизайн заданий и педагогическая логика: [docs/design/assignment_design.md](../design/assignment_design.md).
