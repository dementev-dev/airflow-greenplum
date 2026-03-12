# Документация

Этот каталог содержит дополнительные материалы к учебному стенду.

## Быстрый путь (для менти)

- [Быстрый старт и команды](../README.md)
- [Учебные задания](assignment/README.md)
- [План тестирования и проверки](../TESTING.md)
- [Главный учебный DAG: bookings → stg](bookings_to_gp_stage.md)
- [Учебный DAG: stg -> ods](bookings_to_gp_ods.md)
- [Учебный DAG: ods -> dds](bookings_to_gp_dds.md)
- [Учебный DAG: dds -> dm](bookings_to_gp_dm.md)

## Дизайн (`design/`)

- [Единые конвенции нейминга DWH (служебные поля и SCD)](design/naming_conventions.md)
- [Схема БД всех слоёв DWH](design/db_schema.md)
- [Дизайн-документ STG](design/bookings_stg_design.md)
- [Дизайн-документ ODS](design/bookings_ods_design.md)
- [Дизайн-документ DDS](design/bookings_dds_design.md)
- [Дизайн-документ DM](design/bookings_dm_design.md)

> Полные дизайн-документы (PRD, assignment_design, архитектурные решения) — в ветке `solution`.

## Справочники (`reference/`)

- [Как устроен Docker-стенд (образы, Connections, переменные окружения)](stack.md)
- [Известные проблемы bookings-db](reference/bookings_db_issues.md)
- [QA-план отладки пайплайна](reference/qa-plan.md)
- [Порядок запуска DAG-ов](dag_execution_order.md)
