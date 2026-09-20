# Учебные задания

В стенде уже работает цепочка от Bookings до `dm.sales_report`.
Вы дополните ее загрузками ODS, измерениями DDS и четырьмя витринами DM.
Поля, преобразования и бизнес-правила описаны в [ТЗ](analyst_spec.md).

## Подготовка

Пройдите шаги 1–6 [быстрого старта](../../README.md#быстрый-старт-основной-сценарий-bookings--stg--ods--dds--dm):
поднимите стенд, создайте таблицы, запустите эталонную цепочку и проверьте данные.
Затем прочитайте [руководство о структуре DWH](../design/db_schema.md).
Открывайте [карту](../design/architecture-map.html) рядом с заданием;
для Gitea есть [инструкция открытия локального HTML](../design/db_schema.md#как-открыть-карту).

Таблицы уже созданы, задачи Airflow подключены к SQL-файлам.
В учебных `*_load.sql` и `*_dq.sql` пока стоит `SELECT 1;`.
Вы заменяете эти заглушки; менять DAG и создавать таблицы заново не требуется.

## Первая загрузка: ODS

Начните с `ods.airplanes`, затем добавьте `ods.seats`.
На карте откройте [модели самолетов](../design/architecture-map.html#node=ods.airplanes)
и [места](../design/architecture-map.html#node=ods.seats): источники STG уже заполнены.
Позже обе таблицы понадобятся `dds.dim_airplanes` для характеристик модели
и расчета ее вместимости.

### Готовый пример

Пройдите [разбор stg.airports → ods.airports](../design/reading_the_pipeline.md#ods-airports).
Это близкий пример полного снимка с выбором батча, преобразованиями и дедупликацией.
Для первого задания этого разбора достаточно.

### Самостоятельная работа

Откройте [часть 1 ТЗ](analyst_spec.md#часть-1-ods-слой-operational-data-store).
Сверяйте с ней поля и правила каждой таблицы:

| Объект и требования | Файлы для реализации | DDL для сверки |
|---|---|---|
| [ods.airplanes](analyst_spec.md#11-odsairplanes) | [airplanes_load.sql](../../sql/ods/airplanes_load.sql), [airplanes_dq.sql](../../sql/ods/airplanes_dq.sql) | [airplanes_ddl.sql](../../sql/ods/airplanes_ddl.sql) |
| [ods.seats](analyst_spec.md#12-odsseats) | [seats_load.sql](../../sql/ods/seats_load.sql), [seats_dq.sql](../../sql/ods/seats_dq.sql) | [seats_ddl.sql](../../sql/ods/seats_ddl.sql) |

Пример проверок: [airports_dq.sql](../../sql/ods/airports_dq.sql).
Учтите разницу ключей: у модели это `airplane_code`, у места -
`(airplane_code, seat_no)`.

### Запуск и проверка

1. Дождитесь успешного `bookings_to_gp_stage` из подготовки.
   Если STG уже заполнен, новый запуск для этой работы не нужен.
2. Запустите `bookings_to_gp_ods` целиком. Он выберет согласованный снимок
   в `resolve_stg_batch_id`, затем выполнит
   `load_ods_airplanes → dq_ods_airplanes → load_ods_seats → dq_ods_seats`.
   SQL использует XCom этого запуска, поэтому отдельно от выбора батча
   загрузку запускать не стоит.
3. В Greenplum посмотрите строки обеих таблиц: модели, типы числовых полей,
   ключи мест и метки загрузки. Повторите ODS без нового запуска STG:
   бизнес-строки и количество ключей должны сохраниться без дублей.
4. После реализации обеих таблиц запустите `bookings_validate` и откройте
   группу `validate_ods`. Она проверит наличие данных, совпадение ключей
   с выбранным STG-батчем, отсутствие дублей и NULL в ключах,
   а также общий `_load_id` двух таблиц.

В `bookings_validate` автоматически запускаются все три группы.
Пока готовы только задания ODS, оценивайте `validate_ods`: ошибки DDS и DM
на этом этапе ожидаемы. После выполнения всех заданий должны пройти все группы.

Перед следующей загрузкой дождитесь завершения всего `bookings_validate`
и убедитесь, что `validate_dds.scd2_restore` завершился успешно.
SCD2-тест временно меняет данные маршрута, запускает вашу загрузку
и восстанавливает таблицы из копии. Параллельная загрузка может потерять
свои изменения при восстановлении. Если `scd2_restore` упал, сначала
разберите его лог.

`*_dq.sql` тоже нужно реализовать: успешный `SELECT 1;` ничего не проверяет.

## Следующие этапы

| Этап | Требования | Готовый пример рядом |
|---|---|---|
| DDS: `dim_airplanes`, `dim_passengers` | [Модели](analyst_spec.md#21-ddsdim_airplanes-scd1), [пассажиры](analyst_spec.md#22-ddsdim_passengers-scd1) | [dim_airports_load.sql](../../sql/dds/dim_airports_load.sql) |
| DDS: `dim_routes` | [Версии маршрута SCD2](analyst_spec.md#23-ddsdim_routes-scd2) | [Разбор сборки факта](../design/reading_the_pipeline.md#fact-flight-sales) показывает, как используется версия |
| DM: `airport_traffic` → `route_performance` → `monthly_overview` → `passenger_loyalty` | [Часть 3 ТЗ](analyst_spec.md#часть-3-dm-слой-data-marts--витрины) | [sales_report_load.sql](../../sql/dm/sales_report_load.sql) |

Перед DDS разберите [загрузку факта](../design/reading_the_pipeline.md#fact-flight-sales).
После реализации измерений выполните [пересчет факта и витрин](analyst_spec.md#пересчёт-факта-после-реализации-измерений).
Проверяйте готовый слой своей группой в `bookings_validate`.

## Если застряли

Вернитесь к карточке объекта на карте: там есть DDL, загрузка, DQ и пункт ТЗ.
Для конкретного вопроса используйте [соглашения об именах](../design/naming_conventions.md),
[справочник DQ](../reference/dq_taxonomy.md) и руководства DAG:
[ODS](../bookings_to_gp_ods.md), [DDS](../bookings_to_gp_dds.md), [DM](../bookings_to_gp_dm.md).
Полную реализацию можно посмотреть в ветке `solution`.

## Для менторов

Дизайн заданий и педагогическая логика находятся в ветке `solution`,
в `docs/design/assignment_design.md`.
