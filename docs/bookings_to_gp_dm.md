# DAG `bookings_to_gp_dm`: `dds` → `dm` в Greenplum

Этот DAG — учебный пример загрузки слоя **DM** (Data Mart / витрины) из текущего состояния **DDS**.
Все 5 витрин загружаются **параллельно** и демонстрируют разные стратегии загрузки —
это ключевая учебная ценность данного DAG.

## Что делает DAG

Загружает 5 витрин параллельно, для каждой — пара `load → dq`:

| Витрина | Зерно (grain) | Паттерн загрузки |
|---------|---------------|------------------|
| `dm.sales_report` | (flight_date, departure_airport_sk, arrival_airport_sk, tariff_sk) | Инкрементальный UPSERT (HWM по датам) |
| `dm.route_performance` | route_bk | Full Rebuild (TRUNCATE + INSERT) |
| `dm.passenger_loyalty` | passenger_sk | Инкрементальный UPSERT (HWM по затронутым ключам) |
| `dm.airport_traffic` | (traffic_date, airport_sk) | Инкрементальный UPSERT (HWM по датам) |
| `dm.monthly_overview` | (year_actual, month_actual, airplane_sk) | Инкрементальный UPSERT (HWM по месяцам) |

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

```
start_dm
├── load_dm_sales_report      → dq_dm_sales_report      ─┐
├── load_dm_route_performance → dq_dm_route_performance ─┤
├── load_dm_passenger_loyalty → dq_dm_passenger_loyalty ─┼─ finish_dm_summary
├── load_dm_airport_traffic   → dq_dm_airport_traffic   ─┤
└── load_dm_monthly_overview  → dq_dm_monthly_overview  ─┘
```

Все 5 веток полностью независимы и работают параллельно.

## Как это работает внутри (по шагам)

### 1) `load_dm_sales_report` → `dq_dm_sales_report`

- **SQL:** `sql/dm/sales_report_load.sql`, `sql/dm/sales_report_dq.sql`
- **Паттерн:** инкрементальный UPSERT (HWM по датам).

Витрина агрегирует продажи билетов по дате, аэропортам вылета/прилёта и тарифу.
Инкрементальность работает через HWM: витрина сравнивает свой `MAX(_load_ts)` с `_load_ts`
фактов в DDS и пересчитывает агрегаты только для **затронутых дат**.

> Если витрина пуста — `1900-01-01` заберёт всю историю (первичная загрузка).
> Если DAG не запускался несколько дней — при следующем запуске витрина автоматически
> «догонит» всю накопленную дельту.

Учебные приёмы:
- **TEMP TABLE** для однократной агрегации (канон для MPP);
- **NULLIF** для защиты от деления на ноль (`boarding_rate = boarded / NULLIF(sold, 0)`);
- **Денормализация**: города и коды аэропортов тянутся в витрину из измерений.

### 2) `load_dm_route_performance` → `dq_dm_route_performance`

- **SQL:** `sql/dm/route_performance_load.sql`, `sql/dm/route_performance_dq.sql`
- **Паттерн:** Full Rebuild (TRUNCATE + INSERT).

Витрина агрегирует эффективность маршрутов за всю историю. Таблица маленькая (~1000 строк),
поэтому пересоздать её с нуля дешевле, чем вычислять дельту. Дополнительная причина:
таблица хранится в формате **AO Column Store**, который не поддерживает эффективный UPDATE/DELETE.

Трёхшаговый паттерн:
1. **TRUNCATE** — очистка (единственный эффективный способ для AO).
2. **Агрегация** по `route_bk` — факты суммируются через **все исторические версии** маршрута
   (route_sk из SCD2), чтобы не терять данные при версионировании.
3. **JOIN** с текущей (актуальной, `valid_to IS NULL`) версией `dim_routes` для денормализации.

> Благодаря денормализации `dim_routes` — один JOIN вместо четырёх
> (аэропорты вылета/прилёта, самолёт уже хранятся в `dim_routes`).

Метрики: `avg_load_factor = total_boarded / (total_flights * total_seats)`,
`avg_ticket_price = total_revenue / total_tickets`.

### 3) `load_dm_passenger_loyalty` → `dq_dm_passenger_loyalty`

- **SQL:** `sql/dm/passenger_loyalty_load.sql`, `sql/dm/passenger_loyalty_dq.sql`
- **Паттерн:** инкрементальный UPSERT по «затронутым ключам».

В отличие от `sales_report` (где инкремент по датам), здесь HWM находит **конкретных пассажиров**
с новыми фактами, а затем пересчитывает для них всю историю. Это гарантирует точность
накопительных агрегатов (`total_spent`, `first/last_flight_date`).

Учебные приёмы:
- **DISTINCT ON** (PostgreSQL-специфика) для нахождения моды (самый частый тариф пассажира);
- **Агрегация SCD2 по BK**: при подсчёте уникальных маршрутов используем `route_bk`,
  а не `route_sk`, т.к. один маршрут может иметь несколько версий;
- Фильтрация `passenger_sk IS NOT NULL` — защита от неконсистентных фактов (NULL SK
  при data quality аномалиях в измерениях).

### 4) `load_dm_airport_traffic` → `dq_dm_airport_traffic`

- **SQL:** `sql/dm/airport_traffic_load.sql`, `sql/dm/airport_traffic_dq.sql`
- **Паттерн:** инкрементальный UPSERT по датам.

Витрина показывает пассажиропоток аэропортов по дням (вылеты + прилёты).

Учебный приём — **Dual-role dimension через UNION ALL**: один билет превращается
в два «события» (вылет из одного аэропорта и прилёт в другой).
Это позволяет собрать единую статистику аэропорта (departures + arrivals) в одном проходе.

### 5) `load_dm_monthly_overview` → `dq_dm_monthly_overview`

- **SQL:** `sql/dm/monthly_overview_load.sql`, `sql/dm/monthly_overview_dq.sql`
- **Паттерн:** инкрементальный UPSERT по месяцам.

Витрина показывает помесячную статистику по типам самолётов.

Учебный приём — **двухуровневая агрегация**: чтобы честно посчитать `avg_load_factor`,
сначала считаем load factor для каждого рейса (`boarded / total_seats`),
затем берём среднее по месяцу. Прямая агрегация `SUM(boarded) / SUM(seats)` дала бы
искажённый результат (взвешенный по числу билетов, а не рейсов).

> **Ограничение SCD1:** `total_seats` берётся из текущего состояния `dim_airplanes`.
> Если самолёт переоборудовали в прошлом, для точного исторического расчёта
> потребовалось бы SCD2-измерение.

### 6) `start_dm` / `finish_dm_summary`

- `start_dm` — стартовый sentinel, от которого расходятся все 5 параллельных веток.
- `finish_dm_summary` — ждёт завершения всех DQ-задач и логирует сводку.

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

-- Инвариант sales_report: посаженных не больше, чем продано
SELECT COUNT(*) FROM dm.sales_report WHERE tickets_sold < passengers_boarded;

-- Нет дублей по бизнес-ключу route_performance
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
