# Загрузка Bookings → STG: `bookings_to_gp_stage`

DAG читает девять таблиц Bookings через внешние таблицы PXF и сохраняет
строки в Greenplum. Весь STG, включая DQ, уже реализован на `main`.
Смысл бронирования, билета и сегмента разобран в
[руководстве о структуре](design/db_schema.md); на карте начните со
[stg.bookings](design/architecture-map.html#node=stg.bookings).
Карту открывайте [локально в браузере](design/db_schema.md#как-открыть-карту).

## Перед запуском

Выполните подготовку из [быстрого старта](../README.md#быстрый-старт-основной-сценарий-bookings--stg--ods--dds--dm):
стенд должен работать, база `demo` содержать исходные данные,
а DAG `bookings_stg_ddl` создать внутренние таблицы `stg.*` и внешние `stg.*_ext`.
Альтернатива DDL DAG: `make ddl-gp`, который создает все четыре слоя.

Подключения `bookings_db` и `greenplum_conn` заданы через `AIRFLOW_CONN_...`
в Compose и могут не отображаться в списке Connections.
Порты, учетные данные и настройка PXF описаны в [справочнике стенда](stack.md).

В Airflow UI (по умолчанию http://localhost:8080) запустите
`bookings_to_gp_stage` через Trigger DAG и дождитесь завершения всех задач.
Каждый полный запуск добавляет следующий учебный день в источник.
Дата запуска Airflow не задает день генерируемых данных.

## Какие строки загружаются

[generate_bookings_day](../sql/src/bookings_generate_day_if_missing.sql)
вызывает `continue(...)` до конца следующего дня после `MAX(book_date)`.
Для пустой установленной базы он вызывает `generate(...)`, используя настройки
`bookings.start_date`, `bookings.init_days` и `bookings.jobs`.
Эти настройки задаются при инициализации Bookings.

Внешняя таблица `*_ext` читает источник; внутренний STG хранит полученные строки.
Бизнес-поля сохраняются как TEXT. `_load_id` равен `run_id` запуска STG,
`_load_ts` фиксирует время записи. Опорное `event_ts` зависит от таблицы:

| Таблицы | Отбор строк | Готовый SQL |
|---|---|---|
| `bookings` | Новые `book_date` | [bookings_load.sql](../sql/stg/bookings_load.sql) |
| `tickets`, `segments`, `boarding_passes` | По `book_date` связанного бронирования через билеты | [tickets](../sql/stg/tickets_load.sql), [segments](../sql/stg/segments_load.sql), [boarding_passes](../sql/stg/boarding_passes_load.sql) |
| `flights` | Новые `scheduled_departure` | [flights_load.sql](../sql/stg/flights_load.sql) |
| `airports`, `airplanes`, `routes`, `seats` | Полный снимок при каждом запуске; `event_ts = now()` | [airports](../sql/stg/airports_load.sql), [airplanes](../sql/stg/airplanes_load.sql), [routes](../sql/stg/routes_load.sql), [seats](../sql/stg/seats_load.sql) |

У каждой инкрементальной таблицы своя нижняя граница:
`MAX(event_ts)` по предыдущим батчам этой таблицы. Верхней границы нет.
`NOT EXISTS` не дает повторно вставить тот же ключ в текущий `_load_id`.
У справочников сохраняются снимки разных запусков, поэтому повторение
бизнес-ключа между батчами ожидаемо.

Такой инкремент не перечитывает все старые строки источника: например,
изменение рейса с датой ниже границы само по себе в новый STG-батч не попадет.
Для проверки повторяемости загрузки учитывайте и генератор:
повторный запуск `generate_bookings_day` снова продвинет источник на день.

## Порядок задач

В [DAG](../airflow/dags/bookings_to_gp_stage.py) после каждой загрузки стоит DQ.
Ниже стрелки показывают порядок выполнения задач Airflow.
Связи строк по ключам смотрите на карте.

1. `generate_bookings_day → load_bookings_to_stg → check_row_counts`.
2. `load_tickets_to_stg → check_tickets_dq`.
3. После билетов параллельно могут выполняться загрузки аэропортов и моделей.
   `load_routes_to_stg` ждет `check_airports_dq` и `check_airplanes_dq`;
   `load_seats_to_stg` ждет только `check_airplanes_dq`.
4. После проверки маршрутов идет цепочка
   `flights → segments → boarding_passes`, у каждой таблицы своя DQ-задача.
5. `finish_summary` ждет `check_boarding_passes_dq` и `check_seats_dq`.

Инкрементальные DQ допускают пустое окно: выводят `NOTICE` и завершаются успешно.
Пустой источник справочника считается ошибкой. Сверки выполняются для текущего
батча; примеры: [bookings_dq.sql](../sql/stg/bookings_dq.sql) и
[airports_dq.sql](../sql/stg/airports_dq.sql).

## Проверка результата

Откройте Greenplum командой `make gp-psql`:

```sql
SELECT COUNT(*) FROM stg.bookings;

SELECT book_ref, book_date, event_ts, _load_ts, _load_id
FROM stg.bookings
ORDER BY _load_ts DESC, book_ref
LIMIT 10;
```

После первого успешного запуска `stg.bookings` непуста. Сопоставьте `_load_id`
с Run ID в Airflow и объясните, чем `book_date` отличается от `_load_ts`.
Для просмотра первоисточника используйте `make bookings-psql`.

Далее запускайте [ODS](bookings_to_gp_ods.md).
Общий порядок слоев находится в [порядке запуска DAG](dag_execution_order.md).

## Если загрузка не прошла

Откройте Log первой упавшей задачи в Airflow. Если упала DQ, начните с ключа
и батча, названных в сообщении, затем откройте ее SQL из карточки таблицы.

- Нет базы `demo`: пройдите инициализацию из README. `make bookings-init`
  пересоздает исходную базу из seed-дампа, поэтому не используйте его для
  обычного повторного запуска уже заполненного стенда.
- Нет `stg.*` или `stg.*_ext`: выполните `bookings_stg_ddl`.
- Ошибка PXF или соединения: проверьте `docker compose ps`, логи `greenplum`
  и `bookings-db`, затем настройки из [справочника стенда](stack.md).
  Повторное применение DDL само по себе не исправляет недоступное подключение.
