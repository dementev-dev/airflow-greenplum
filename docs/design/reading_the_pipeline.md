# От карты к SQL загрузки

На [карте](architecture-map.html) найдите таблицу, которую хотите заполнить,
и откройте ее SQL загрузки. Проследите, какие строки скрипт читает,
что меняет и по какому ключу записывает результат.
Смысл строк и связей разобран в [руководстве о структуре DWH](db_schema.md).

Для заданий на снимки ODS достаточно примера `ods.airports` ниже.
К сборке факта вернитесь перед заданиями DDS.
Адресные ссылки на карту работают в локальном просмотре;
если читаете в Gitea, используйте [инструкцию открытия карты](db_schema.md#как-открыть-карту).

<a id="ods-airports"></a>

## Справочник: stg.airports → ods.airports

Откройте [ods.airports на карте](architecture-map.html#node=ods.airports)
и связь [Типизация и загрузка ODS](architecture-map.html#edge=flow-stg.airports-ods.airports-13).
Здесь один источник: `stg.airports`. В STG накапливаются снимки разных запусков,
а в ODS нужна одна строка на аэропорт из выбранного снимка.

Рядом откройте [airports_load.sql](../../sql/ods/airports_load.sql).
Пройдите скрипт по порядку:

1. `TRUNCATE TABLE ods.airports` очищает предыдущий снимок.
   Следующий `INSERT` заменяет его целиком, поэтому исчезнувший из нового
   снимка аэропорт исчезнет и из ODS.
2. В CTE `src` найдите `FROM stg.airports` и фильтр `_load_id`:

   ```sql
   WHERE s._load_id = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'::text
   ```

   Задача `resolve_stg_batch_id` в [ODS DAG](../../airflow/dags/bookings_to_gp_ods.py)
   выбирает общий батч четырех справочников STG или берет `stg_batch_id`
   из параметров запуска. Этот фильтр оставляет один снимок из истории STG.
3. Выражения `airport_name::json->>'ru'`, `city::json->>'ru'`
   и `country::json->>'ru'` извлекают русские названия. Остальные бизнес-поля
   переносятся без преобразования.
4. `ROW_NUMBER()` нумерует строки внутри `airport_code` по `_load_ts DESC`,
   затем `event_ts DESC NULLS LAST`. Финальный `WHERE s.rn = 1`
   оставляет одну строку на код.
5. Сопоставьте колонки `INSERT INTO ods.airports (...)` с финальным `SELECT`.
   В `_load_id` сохраняется идентификатор выбранного STG-батча,
   в `_load_ts` записывается время загрузки ODS. Поля и типы результата
   заданы в [airports_ddl.sql](../../sql/ods/airports_ddl.sql).
6. `ANALYZE` обновляет статистику таблицы для планировщика запросов.
   Проверки находятся отдельно: [airports_dq.sql](../../sql/ods/airports_dq.sql)
   сравнивает множества ключей STG-батча и ODS, ищет дубли и пустые обязательные поля.

Повторная загрузка того же снимка должна сохранить бизнес-строки без дублей;
время `_load_ts` при этом обновится.
Теперь переходите к [первой самостоятельной загрузке](../assignment/README.md#первая-загрузка-ods).

<a id="пример-fact_flight_sales"></a>
<a id="fact-flight-sales"></a>

## Факт: сегмент билета и его измерения

Откройте [dds.fact_flight_sales](architecture-map.html#node=dds.fact_flight_sales)
и [fact_flight_sales_load.sql](../../sql/dds/fact_flight_sales_load.sql).
Одна строка факта соответствует паре `(ticket_no, flight_id)`.
В [примере поездки](db_schema.md#сборка-факта-продаж) два билета с тремя
сегментами дают три строки факта.

<a id="метод"></a>
<a id="fact-joins"></a>

### Найдите источник новых строк

В файле сначала идет `UPDATE`, а затем CTE `fact_src` и `INSERT`.
Начните с `fact_src`: здесь виден полный набор источников новых строк.
`FROM ods.segments AS seg` задает зерно, а `seg.amount AS price`
переносит цену сегмента в факт.

| Участок `fact_src` | Что добавляется к сегменту | Связь на карте |
|---|---|---|
| `JOIN ods.tickets`, затем `JOIN ods.bookings` | Бронирование, пассажир и дата покупки | [Билет и пассажир](architecture-map.html#edge=fact-1), [дата бронирования](architecture-map.html#edge=fact-2) |
| `JOIN ods.flights` | Дата вылета и номер маршрута | [Рейс, дата и маршрут](architecture-map.html#edge=fact-3) |
| `LEFT JOIN` подзапроса из `ods.routes` | Коды аэропортов из строки с наибольшим `validity` на `route_no` | [Коды двух аэропортов](architecture-map.html#edge=fact-4) |
| Два `LEFT JOIN dds.dim_airports` как `dep` и `arr` | `departure_airport_sk` и `arrival_airport_sk` по двум кодам | [Вылет](architecture-map.html#edge=fact-7), [прилет](architecture-map.html#edge=fact-8) |
| `LEFT JOIN dds.dim_routes`, затем `dds.dim_airplanes` | Версия маршрута на дату рейса и SK ее модели самолета | [Версия маршрута](architecture-map.html#edge=fact-5), [модель](architecture-map.html#edge=fact-9) |
| `LEFT JOIN dds.dim_calendar`, `dds.dim_tariffs`, `dds.dim_passengers` | SK по дате вылета, классу обслуживания и `passenger_id` | [Дата](architecture-map.html#edge=fact-6), [тариф](architecture-map.html#edge=fact-10), [пассажир](architecture-map.html#edge=fact-11) |
| `LEFT JOIN ods.boarding_passes` по обоим полям ключа | `seat_no`; наличие талона задает `is_boarded` | [Посадка и место](architecture-map.html#edge=fact-12) |

Если билет, бронирование или рейс не найдены, `INNER JOIN` исключит сегмент
из вставки. `LEFT JOIN` сохранит его при отсутствии совпадения: например,
без талона получатся `seat_no = NULL` и `is_boarded = false`.

Обратите внимание на два разных пути от маршрута:

- Аэропорты берутся через готовый `ods.routes`. Отбор `ORDER BY validity DESC`
  работает по полю TEXT и не использует дату рейса.
- `route_sk` ищется в студенческом `dim_routes` по дате вылета
  в интервале `[valid_from, valid_to)`. Из этой версии берется `airplane_code`
  для поиска `airplane_sk`.

До выполнения заданий `route_sk`, `airplane_sk` и `passenger_sk` могут быть
`NULL`. Путь аэропортов от студенческих измерений не зависит.
Объяснение этой части модели: [аэропорты и версия маршрута](db_schema.md#аэропорты-вылета-и-прилета).

### Найдите запись результата

Финальный `INSERT` берет поля из `fact_src`. Условие `NOT EXISTS`
проверяет пару `(ticket_no, flight_id)` и пропускает уже записанные строки.
Вернитесь к `UPDATE` в начале файла: он меняет только цену, место,
признак посадки и метки загрузки у существующих строк.
Ключи измерений он не пересчитывает.

Поэтому после реализации измерений нужен
[пересчет факта по ТЗ](../assignment/analyst_spec.md#пересчёт-факта-после-реализации-измерений).
Проверки результата находятся в [fact_flight_sales_dq.sql](../../sql/dds/fact_flight_sales_dq.sql).

## Как проверить свой слой

Перед записью SQL найдите свой объект на карте и откройте его пункт ТЗ.
Сформулируйте, что означает одна строка результата, по каким полям соединяются
источники и что произойдет при отсутствии совпадения. Затем сверяйте
`INSERT` с DDL, а преобразования и `*_dq.sql` с требованиями задания.

Близкие готовые примеры для следующих этапов:

- SCD1: [dim_airports_load.sql](../../sql/dds/dim_airports_load.sql) обновляет
  изменившиеся атрибуты через `IS DISTINCT FROM` и вставляет новые бизнес-ключи.
- DM: [sales_report_load.sql](../../sql/dm/sales_report_load.sql) находит
  затронутые даты по `_load_ts` факта, пересчитывает за них полные агрегаты
  во временную таблицу и выполняет `UPDATE` и `INSERT`.

[Порядок заданий](../assignment/README.md#следующие-этапы) ведет к требованиям
каждого этапа. Имена служебных полей проверяйте по
[соглашениям](naming_conventions.md), состав DQ - по
[справочнику проверок](../reference/dq_taxonomy.md).
