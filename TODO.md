# TODO (maintainers / mentors)

Этот файл собирает идеи по доработке стенда, которые не критичны для текущих задач менти,
но улучшат стабильность и удобство сопровождения.

- Собрать свой образ Airflow поверх `apache/airflow:2.9.2`:
  - вынести установку Python‑зависимостей из runtime (`pip install ...` при старте контейнеров)
    в отдельный `Dockerfile`;
  - переключить `docker-compose.yml` на использование этого образа для `airflow-webserver`,
    `airflow-scheduler` и `airflow-init`;
  - обновить документацию (README/TESTING) под новую схему сборки.

- Переключить Airflow с `SequentialExecutor` (SequentialScheduler) на `LocalExecutor`
  для docker‑стенда:
  - проверить, какие параметры достаточно поменять в env/конфиге (`AIRFLOW__CORE__EXECUTOR`)
    для образа `apache/airflow:2.9.2`;
  - убедиться, что примерные DAG’и (`csv_to_greenplum`, `bookings_to_gp_stage`) ведут себя
    предсказуемо в режиме параллельного исполнения;
  - при необходимости скорректировать тесты и документацию (README/TESTING) с учётом нового executor’а.
