# Учебные задания

## Техническое задание

Основной документ: **[analyst_spec.md](analyst_spec.md)** — ТЗ от аналитика
с описанием всех таблиц, маппингами, бизнес-правилами и подсказками.

## Эталон для изучения

Перед началом работы изучите эталонный срез (витрина `dm.sales_report`
и вся её цепочка STG → ODS → DDS → DM):

- SQL-скрипты: `sql/stg/`, `sql/ods/`, `sql/dds/`, `sql/dm/`
- DAG-файлы: `airflow/dags/`
- [Порядок запуска DAG-ов](../reference/dag_execution_order.md)

## Для менторов

Дизайн заданий и педагогическая логика: [docs/design/assignment_design.md](../design/assignment_design.md).
