# Документация

## Знакомство и первое задание

После [запуска стенда](../README.md) пройдите три шага:

1. [Структура DWH](design/db_schema.md): смысл строк и связей на примере поездки.
2. [От карты к SQL загрузки](design/reading_the_pipeline.md#ods-airports):
   разбор готового справочника ODS. Сборка факта понадобится позже, перед DDS.
3. [Учебные задания](assignment/README.md): файлы для работы, порядок запуска
   и проверка результата. Поля и бизнес-правила находятся в [ТЗ](assignment/analyst_spec.md).

## Найти деталь во время работы

[Интерактивная карта](design/architecture-map.html) ведет от таблицы к ее
источникам, потребителям, SQL и пункту задания.

Карту открывайте из локальной копии: `docs/design/architecture-map.html` в браузере.
Сервер не нужен. Просмотр исходного HTML в Gitea не показывает карту.
[Руководство](design/db_schema.md#как-открыть-карту) объясняет открытие и адресные ссылки.

Если вопрос о запуске или проверке, откройте нужный материал:

- [Порядок запуска DAG](dag_execution_order.md) и [план проверки стенда](../TESTING.md).
- Руководства загрузок: [STG](bookings_to_gp_stage.md), [ODS](bookings_to_gp_ods.md),
  [DDS](bookings_to_gp_dds.md), [DM](bookings_to_gp_dm.md).
- [Соглашения об именах](design/naming_conventions.md) и [классы DQ-проверок](reference/dq_taxonomy.md).

## Дизайн (`design/`)

- [Единые конвенции нейминга DWH (служебные поля и SCD)](design/naming_conventions.md)
- [Структура DWH и маршрут по карте](design/db_schema.md)
- [От карты к SQL загрузки](design/reading_the_pipeline.md)
- [Дизайн-документ STG](design/bookings_stg_design.md)
- [Дизайн-документ ODS](design/bookings_ods_design.md)
- [Дизайн-документ DDS](design/bookings_dds_design.md)
- [Дизайн-документ DM](design/bookings_dm_design.md)

> Полные дизайн-документы (PRD, assignment_design, архитектурные решения) — в ветке `solution`.

## Справочники (`reference/`)

- [Как устроен Docker-стенд (образы, Connections, переменные окружения)](stack.md)
- [Известные проблемы bookings-db](reference/bookings_db_issues.md)
- [QA-план отладки пайплайна](reference/qa-plan.md)
- [Классы DQ-проверок](reference/dq_taxonomy.md)
- [Порядок запуска DAG-ов](dag_execution_order.md)
