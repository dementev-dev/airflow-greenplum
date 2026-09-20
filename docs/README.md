# Документация

Этот каталог содержит дополнительные материалы к учебному стенду.

## Быстрый путь (для менти)

- [Быстрый старт и команды](../README.md)
- [Структура DWH: от бронирования до витрины](design/db_schema.md)
- [Интерактивная карта таблиц и связей](design/architecture-map.html)
- [Учебные задания](assignment/README.md)
- [Как читать пайплайн по скрипту загрузки](design/reading_the_pipeline.md)
- [План тестирования и проверки](../TESTING.md)
- [Главный учебный DAG: bookings → stg](bookings_to_gp_stage.md)
- [Учебный DAG: stg -> ods](bookings_to_gp_ods.md)
- [Учебный DAG: ods -> dds](bookings_to_gp_dds.md)
- [Учебный DAG: dds -> dm](bookings_to_gp_dm.md)

Карту открывайте из локальной копии: `docs/design/architecture-map.html` в браузере.
Сервер не нужен. Просмотр исходного HTML в Gitea не показывает карту.
[Руководство](design/db_schema.md#как-открыть-карту) объясняет открытие и адресные ссылки.

## Дизайн (`design/`)

- [Единые конвенции нейминга DWH (служебные поля и SCD)](design/naming_conventions.md)
- [Структура DWH и маршрут по карте](design/db_schema.md)
- [Как читать пайплайн по скрипту загрузки](design/reading_the_pipeline.md)
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
