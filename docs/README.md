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
- [Архитектурные решения (ADR)](design/architecture_review.md)
- [PRD: стратегия курсовой](design/PRD.md)
- [Дизайн задания](design/assignment_design.md)

## Справочники (`reference/`)

- [Как устроен Docker-стенд (образы, Connections, переменные окружения)](stack.md)
- [PXF в этом проекте (проектная реализация)](reference/pxf_bookings.md)
- [Про время/UTC в bookings](reference/bookings_tz.md)
- [Известные проблемы bookings-db](reference/bookings_db_issues.md)
- [Бенчмарк генерации данных](reference/bookings_generation_benchmark.md)
- [QA-план отладки пайплайна](reference/qa-plan.md)
- [Порядок запуска DAG-ов](dag_execution_order.md)
- [End-to-end протокол тестирования](e2e-etl-test-protocol.md)
- [Тестирование DAG-ов через API](agent-dag-testing.md)

## Планы (`plans/`)

Активные планы работ. После выполнения переносятся в `archive/`.

## Архив (`archive/`)

Выполненные планы, закрытые ревью. Ссылки внутри файлов могут быть устаревшими.
