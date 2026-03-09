# Документация

Этот каталог содержит дополнительные материалы к учебному стенду.

## Быстрый путь (для менти)

- [Быстрый старт и команды](../README.md)
- [Учебные задания](../educational-tasks.md)
- [План тестирования и проверки](../TESTING.md)
- [Главный учебный DAG: bookings → stg](bookings_to_gp_stage.md)
- [Учебный DAG: stg -> ods](bookings_to_gp_ods.md)
- [Учебный DAG: ods -> dds](bookings_to_gp_dds.md)
- [Учебный DAG: dds -> dm](bookings_to_gp_dm.md)

## Технические детали (опционально)

- [Как устроен Docker-стенд (образы, Connections, переменные окружения)](stack.md)
- [Единые конвенции нейминга DWH (служебные поля и SCD)](internal/naming_conventions.md)
- [PXF в этом проекте (проектная реализация)](internal/pxf_bookings.md)
- [Дизайн-документ STG](internal/bookings_stg_design.md)
- [Дизайн-документ ODS](internal/bookings_ods_design.md)
- [Дизайн-документ DDS](internal/bookings_dds_design.md)
- [Дизайн-документ DM](internal/bookings_dm_design.md)
- [Про время/UTC в bookings](internal/bookings_tz.md)
