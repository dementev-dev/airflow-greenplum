# Загрузка ODS → DDS: `bookings_to_gp_dds`

DAG наполняет измерения и собирает `dds.fact_flight_sales`.
Одна строка факта соответствует сегменту билета: `(ticket_no, flight_id)`.
У одного билета может быть несколько таких строк.
Сборка показана [на карте](design/architecture-map.html#node=dds.fact_flight_sales)
и в [разборе SQL](design/reading_the_pipeline.md#fact-flight-sales).
Карту открывайте [локально](design/db_schema.md#как-открыть-карту).

## Перед запуском

Дождитесь успешных STG и ODS. Таблицы DDS должны быть созданы через
`bookings_dds_ddl` или общий `make ddl-gp`.
Затем в Airflow UI запустите `bookings_to_gp_dds` целиком.
DDS читает состояние ODS; передавать `stg_batch_id` ему не нужно.

| Объект | Что уже выполняется на `main` |
|---|---|
| `dim_calendar` | Заполнение датами с 2016-01-01 по 2030-12-31, только если таблица пуста |
| `dim_airports` | Обновление изменившихся атрибутов и вставка новых аэропортов с постоянными SK |
| `dim_tariffs` | Добавление новых классов обслуживания из `ods.segments` |
| `fact_flight_sales` | Обновление существующих сегментов и вставка новых пар `(ticket_no, flight_id)` |
| `dim_airplanes`, `dim_passengers`, `dim_routes` | Загрузки и DQ пока содержат `SELECT 1;`; требования находятся в [части 2 ТЗ](assignment/analyst_spec.md#часть-2-dds-слой-detailed-data-store--измерения) |

До выполнения заданий три студенческих измерения остаются пустыми.
Готовый факт при этом загружается: отсутствующие ключи измерений сохраняются
как `NULL`, чтобы можно было изучить цепочку до написания собственного SQL.

## Порядок задач

В [DAG](../airflow/dags/bookings_to_gp_dds.py) сначала выполняется
`load_dds_dim_calendar → dq_dds_dim_calendar`. Затем:

1. Параллельно могут загружаться `dim_airports`, `dim_airplanes`, `dim_tariffs`
   и `dim_passengers`; у каждого измерения своя следующая DQ-задача.
2. `load_dds_dim_routes` ждет `dq_dds_dim_airports` и `dq_dds_dim_airplanes`,
   затем выполняется `dq_dds_dim_routes`.
3. `load_dds_fact_flight_sales` ждет DQ всех пяти измерений после календаря.
4. `dq_dds_fact_flight_sales → finish_dds_summary` завершают запуск.

Это порядок задач Airflow. Источники строк и поля соединений показаны
на карте отдельно от него.

## Как работают готовые загрузки

[dim_calendar_load.sql](../sql/dds/dim_calendar_load.sql) проверяет
`NOT EXISTS (SELECT 1 FROM dds.dim_calendar LIMIT 1)`. Если есть хотя бы одна
строка, скрипт не вставляет даты. Повторный запуск не пересоздает календарь
и не дополняет частично заполненную таблицу.
[Его DQ](../sql/dds/dim_calendar_dq.sql) проверяет также покрытие дат рейсов ODS.

[dim_airports_load.sql](../sql/dds/dim_airports_load.sql) сохраняет SK
существующего аэропорта, обновляет изменившиеся атрибуты через
`IS DISTINCT FROM` и вставляет новые бизнес-ключи.
[dim_tariffs_load.sql](../sql/dds/dim_tariffs_load.sql) только добавляет
недостающие значения `fare_conditions`.
Метки измененных строк относятся к текущему запуску DDS;
календарь не содержит `_load_id` и `_load_ts`.

В [fact_flight_sales_load.sql](../sql/dds/fact_flight_sales_load.sql)
есть две операции:

- `UPDATE` меняет цену, место, признак посадки и метки загрузки, если эти
  бизнес-поля изменились. SK измерений остаются прежними.
- `INSERT` собирает данные из ODS и измерений для пар
  `(ticket_no, flight_id)`, которых еще нет в факте.

Факт не отбирает дельту по HWM. Скрипт читает ODS и сопоставляет бизнес-ключи
с уже записанными строками.

Аэропорты вылета и прилета находятся через `ods.routes → dim_airports`.
Из ODS выбирается строка с наибольшим `validity` для `route_no`; сортировка
идет по TEXT, дата рейса в этом отборе не участвует.
`route_sk` определяется отдельно: по дате вылета в полуоткрытом интервале
`[valid_from, valid_to)` измерения `dim_routes`. Из найденной версии берется
код модели для поиска `airplane_sk`.
[Разбор соединений](design/reading_the_pipeline.md#fact-joins) показывает,
какие строки сохраняет `LEFT JOIN` и что происходит при отсутствии измерения.

## Учебные измерения

Начните с [моделей самолетов](assignment/analyst_spec.md#21-ddsdim_airplanes-scd1)
и [пассажиров](assignment/analyst_spec.md#22-ddsdim_passengers-scd1), затем
переходите к [версиям маршрута](assignment/analyst_spec.md#23-ddsdim_routes-scd2).
Поля, hashdiff и алгоритм SCD2 определены в ТЗ.

У маршрута изменение версионируемых атрибутов создает новую строку.
Описательные поля из других измерений обновляются только у открытых версий
(`valid_to IS NULL`), без новой версии; закрытые версии сохраняют свои значения.
Это правило также записано в [алгоритме ТЗ](assignment/analyst_spec.md#алгоритм-scd2-пошагово).

После реализации измерений выполните
[пересчет факта и витрин](assignment/analyst_spec.md#пересчёт-факта-после-реализации-измерений).
Обычный повторный запуск DDS не заполнит SK в старых строках факта,
поскольку его `UPDATE` эти поля не меняет.

## Проверка результата

В `make gp-psql` выполните:

```sql
SELECT COUNT(*) FROM dds.dim_calendar;
SELECT COUNT(*) FROM dds.dim_airports;
SELECT COUNT(*) FROM dds.dim_tariffs;

SELECT
    (SELECT COUNT(*) FROM dds.fact_flight_sales) AS fact_rows,
    (SELECT COUNT(*) FROM ods.segments) AS segment_rows;

SELECT ticket_no, flight_id, departure_airport_sk, arrival_airport_sk,
       route_sk, airplane_sk, passenger_sk, price, is_boarded
FROM dds.fact_flight_sales
ORDER BY ticket_no, flight_id
LIMIT 10;
```

На подготовленном стенде готовые измерения и факт непусты.
`fact_rows` должен точно совпасть с `segment_rows`.
[Готовая DQ факта](../sql/dds/fact_flight_sales_dq.sql) также ищет дубли
по паре ключей и проверяет обязательные поля.

На учебной ветке `NULL` в `passenger_sk`, `route_sk` и `airplane_sk`
вызывает только `NOTICE`. `NULL` в `tariff_sk` вызывает ошибку;
для строк с отсутствующим аэропортом и для `calendar_sk` допустима доля
не выше 1%. Эти послабления не заменяют проверку заполнения ключей после задания.

Свои `*_dq.sql` реализуйте по ТЗ и
[справочнику DQ](reference/dq_taxonomy.md). Для дополнительной проверки DDS
используйте `validate_dds` в `bookings_validate`, соблюдая
[порядок запуска и восстановления](assignment/README.md#запуск-и-проверка).
Далее переходите к [DM](bookings_to_gp_dm.md).

## Если загрузка не прошла

- Нет `dds.*`: выполните `bookings_dds_ddl`.
- DQ календаря сообщает о непокрытых датах: сравните даты вылета ODS
  с диапазоном и содержимым `dim_calendar`. Повторный запуск загрузки
  непустого календаря не заполнит пропуски.
- Число строк факта отличается от ODS: проверьте ключи билета, бронирования
  и рейса, затем соединения в `fact_src`. Потеря строки на `INNER JOIN`
  и размножение строк из-за нескольких подходящих версий дают разные причины
  одного расхождения.
- Ошибка в вашей DQ маршрутов: проверьте открытые версии, интервалы и hashdiff
  по ТЗ. Успешная заглушка `SELECT 1;` этих свойств не проверяет.
