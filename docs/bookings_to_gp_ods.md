# Загрузка STG → ODS: `bookings_to_gp_ods`

DAG приводит данные STG к целевым типам и оставляет одну строку на бизнес-ключ.
Снимки справочников заменяют предыдущее состояние; транзакционные таблицы
получают новые и измененные строки. Смысл ключей показан на
[карте ODS](design/architecture-map.html#node=ods.airports),
а готовый снимок разобран в [stg.airports → ods.airports](design/reading_the_pipeline.md#ods-airports).
Карту открывайте [локально](design/db_schema.md#как-открыть-карту).

## Перед запуском

Дождитесь успешного `bookings_to_gp_stage`. Создайте ODS через
`bookings_ods_ddl`, если еще не запускали его или общий `make ddl-gp`.
Подготовка стенда описана в [README](../README.md), последовательность слоев
в [порядке запуска DAG](dag_execution_order.md).

На `main` работают `airports`, `routes`, `bookings`, `tickets`, `flights`,
`segments` и `boarding_passes`. Загрузки и DQ для `airplanes` и `seats`
содержат `SELECT 1;`. Успешный DAG с этими заглушками оставит две таблицы пустыми.
Их нужно заполнить в [первом задании](assignment/README.md#первая-загрузка-ods).

## Запуск и выбор снимка

В Airflow UI запустите `bookings_to_gp_ods` целиком.
Первая задача, `resolve_stg_batch_id`, выбирает последний общий `_load_id`
четырех справочников STG: `airports`, `airplanes`, `routes`, `seats`.
Общие батчи находятся через `INTERSECT`, последний выбирается по времени
загрузки их строк. Результат передается в SQL через XCom.

Resolver проверяет наличие батча в таблицах, а не статус запуска STG в Airflow.
Поэтому сначала дождитесь всех STG DQ. Если нужен определенный снимок,
передайте в Trigger DAG JSON с его реальным `_load_id`:

```json
{"stg_batch_id": "<Run ID успешного запуска STG>"}
```

Явно переданный идентификатор resolver принимает без проверки согласованности.
Убедитесь, что он есть во всех четырех справочниках; произвольная строка
или Run ID запуска ODS здесь не подойдут.

## Что выполняет загрузка

| Объекты | Реализация на `main` | Как отбираются данные |
|---|---|---|
| `airports`, `routes` | Готовые `TRUNCATE + INSERT` | Один снимок по `stg_batch_id`, дедупликация по бизнес-ключу |
| `airplanes`, `seats` | Заглушки; требуемый полный снимок описан в [части 1 ТЗ](assignment/analyst_spec.md#часть-1-ods-слой-operational-data-store) | По выбранному `stg_batch_id` после реализации задания |
| `bookings`, `tickets`, `flights`, `segments`, `boarding_passes` | Готовые `UPDATE + INSERT` через временную таблицу | Новые строки STG по `_load_ts` относительно `MAX(_load_ts)` соответствующей ODS-таблицы |

В [airports_load.sql](../sql/ods/airports_load.sql) и
[routes_load.sql](../sql/ods/routes_load.sql) сначала очищается таблица,
затем вставляется выбранный снимок. Вместе с заменой исчезают ключи,
которых больше нет в снимке. Это один полный пересчет небольшой AO-таблицы;
отдельного `DELETE` для синхронизации ключей нет.

Для транзакционных таблиц, например [bookings](../sql/ods/bookings_load.sql),
дельта фиксируется во временной таблице до `UPDATE` и `INSERT`.
Поэтому обе операции читают один набор строк, даже когда обновление меняет
максимальную `_load_ts` в ODS. Дельта может охватывать несколько STG-батчей;
параметр `stg_batch_id` ее не ограничивает.

[flights_load.sql](../sql/ods/flights_load.sql) дополнительно читает историю
STG для рейсов, на которые ссылаются новые сегменты. Так сегмент может получить
свой рейс, даже если тот пришел раньше текущего инкремента.

Метки загрузки имеют два варианта:

- У снимков `_load_id` сохраняет выбранный STG-батч, `_load_ts = now()`.
- У транзакционных строк обе метки переносятся из STG. Сохраненная `_load_ts`
  служит границей следующего инкремента.

Названия и смысл служебных полей собраны в
[соглашениях об именах](design/naming_conventions.md).

## Порядок задач на main

В [DAG](../airflow/dags/bookings_to_gp_ods.py) после resolver одновременно
могут стартовать три ветки. В таблице указаны зависимости задач Airflow:

| Загрузка | Что должно завершиться перед ней |
|---|---|
| `load_ods_bookings`, `load_ods_airports`, `load_ods_airplanes` | `resolve_stg_batch_id` |
| `load_ods_tickets` | `dq_ods_bookings` |
| `load_ods_routes` | `dq_ods_airports` |
| `load_ods_seats` | `dq_ods_airplanes` |
| `load_ods_flights` | `dq_ods_routes` |
| `load_ods_segments` | `dq_ods_flights` и `dq_ods_tickets` |
| `load_ods_boarding_passes` | `dq_ods_segments` |

Каждый `load_ods_*` ведет к своему `dq_ods_*`.
`finish_ods_summary` ждет `dq_ods_boarding_passes` и `dq_ods_seats`.
На `main` маршруты не ждут студенческий `airplanes`; проверка ссылки
`routes.airplane_code → airplanes` в их DQ отключена.

## Проверка результата

В `make gp-psql` посмотрите строки готовых таблиц:

```sql
SELECT airport_code, airport_name, city, _load_id, _load_ts
FROM ods.airports
ORDER BY airport_code
LIMIT 10;

SELECT COUNT(*) FROM ods.bookings;
SELECT COUNT(*) FROM ods.tickets;
SELECT COUNT(*) FROM ods.segments;

SELECT book_ref, COUNT(*)
FROM ods.bookings
GROUP BY book_ref
HAVING COUNT(*) > 1;
```

После первого полного запуска готовые таблицы непусты; последний запрос
не должен вернуть строк. У `ods.airports` видны русские названия и выбранный
STG-батч. Сравните его с XCom задачи `resolve_stg_batch_id`.

Готовые DQ проверяют ключи, обязательные поля и связи. Для снимков
[airports_dq.sql](../sql/ods/airports_dq.sql) и
[routes_dq.sql](../sql/ods/routes_dq.sql) ищут и пропущенные, и лишние ключи
относительно выбранного батча. DQ транзакционных таблиц проверяют покрытие
ключей выбранного батча, хотя загрузка читает всю дельту по HWM.
Пустой транзакционный батч допустим, пустой снимок справочника считается ошибкой.

После реализации `airplanes` и `seats` пройдите
[самопроверку первого задания](assignment/README.md#запуск-и-проверка).
Дальше можно переходить к [DDS](bookings_to_gp_dds.md).

## Если загрузка не прошла

- `stg_batch_id не найден`: проверьте, что STG загрузил все четыре справочника.
  Одна заполненная `stg.bookings` для выбора снимка недостаточна.
- Нет `ods.*`: выполните `bookings_ods_ddl`.
- Ошибка приведения типа: в логе найдите поле, затем сравните значение
  в STG с преобразованием в `*_load.sql` и типом в DDL.
- DQ сообщает о пропущенных ключах: сопоставьте выбранный батч, SQL загрузки
  и конкретную связь на карте. Для учебных таблиц проверьте, что заменены
  обе заглушки: загрузка и DQ.
