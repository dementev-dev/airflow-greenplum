# TODO (maintainers / mentors)

Этот файл собирает задачи по подготовке стенда к курсовой работе
и идеи по доработке, которые не критичны для текущих задач менти.

Контекст и стратегия: [docs/internal/PRD.md](docs/internal/PRD.md).
Дизайн задания: [docs/internal/assignment_design.md](docs/internal/assignment_design.md).

---

## Подготовка курсовой

> Дедлайн: ~2-3 недели (первый студент может подойти к курсовой).

### Этап 1. Вынос CSV-пайплайна

**Инструмент:** Sonnet / Gemini / ChatGPT — механическая работа, перенос файлов.

- [x] Перенести в [airflow-manual](https://github.com/dementev-dev/airflow-manual):
  `csv_to_greenplum.py`, `csv_to_greenplum_dq.py`, `ddl_greenplum_base.py`,
  `helpers/greenplum.py`, `sql/base/orders_ddl.sql`, связанные тесты
- [x] Убрать CSV-зависимости из docker-compose / .env (`CSV_DIR`, `CSV_ROWS`)
- [x] Обновить README (убрать упоминания CSV-пайплайна)

### Этап 1.5. Полировка эталона

**Инструмент:** Opus (глубокий анализ кода и контекста проекта)
+ ручное тестирование (make up, запуск DAG'ов, проверка данных).

- [x] Протестировать полный ETL-цикл с нуля
      (make up → bookings-init (восстановление из дампа) → STG → ODS → DDS → DM)
- [x] Прогнать инкремент (bookings-generate-day → повторный запуск DAG'ов)
- [x] Почистить код эталонного среза
- [x] Актуализировать README и документацию
- [x] Убедиться, что `make test` и `make lint` проходят
- [ ] Проверить, что стенд поднимается на чистой машине

### Этап 2. Подготовка main

**Инструмент:** Sonnet — удаление файлов и добавление заглушек
по списку из [assignment_design.md](docs/internal/assignment_design.md).

- [ ] Оставить только эталонный срез (sales_report + цепочка)
- [ ] Убрать реализации таблиц-заданий (airplanes, seats, routes в STG/ODS;
      dim_airplanes, dim_passengers, dim_routes в DDS; 4 витрины DM)
- [ ] Добавить TODO-маркеры / заглушки в DAG'ах для студенческих тасков
- [ ] Обновить `ddl_gp.sql` (убрать `\i` для таблиц-заданий)

### Этап 3. ТЗ от аналитика

**Инструмент:** Opus — нужно глубокое понимание предметной области, маппингов
между слоями, SCD-паттернов и педагогического контекста.

- [ ] Создать `docs/assignment/analyst_spec.md`
- [ ] Для каждой таблицы-задания: имя, описание, поля, маппинг,
      бизнес-правила, тип SCD, гранулярность, distribution key
- [ ] Для dim_routes (SCD2): пошаговый алгоритм текстом, формула hashdiff
- [ ] Рекомендуемый порядок выполнения

### Этап 4. Валидационный DAG

**Инструмент:** Sonnet (шаблонная работа, структура в assignment_design.md)
+ Opus для финальной вычитки.

- [ ] Создать `airflow/dags/bookings_validate.py`
- [ ] Создать SQL-скрипты в `sql/validate/`
- [ ] Таски по слоям: STG, ODS, DDS, DM
- [ ] Дружелюбные сообщения об ошибках с подсказками

### Этап 5. Ветка solution

**Инструмент:** Вручную / Sonnet — реализация уже есть в ветке
`chore/bookings-etl`, нужно собрать в ветку `solution`.

- [ ] Создать ветку `solution` от main (после подготовки)
- [ ] Добавить полные реализации всех таблиц-заданий
- [ ] Проверить, что всё работает end-to-end

---

## Прочее (бэклог)

- [ ] Протестировать устойчивость `bookings-db` после остановки контейнеров:
  - прогнать сценарии `make stop` -> `make up` и `make down` -> `make up`;
  - зафиксировать, ломается ли генератор/данные в `bookings-db`;
  - при необходимости добавить шаги восстановления и обновить документацию.

---

## Выполнено

- [x] Сделать REST API Airflow основным способом тестирования ETL вместо CLI-вызовов
  через `docker compose exec ... airflow ...`:
  - обновить `TESTING.md`, сместив фокус на REST API сценарии;
  - оставить CLI как резервный вариант для локальной отладки;
  - проверить, что шаги тестирования воспроизводимы без входа в контейнер Airflow.

- [x] Собрать свой образ Airflow поверх `apache/airflow:2.9.2`:
  - вынести установку Python‑зависимостей из runtime (`pip install ...` при старте контейнеров)
    в отдельный `Dockerfile`;
  - переключить `docker-compose.yml` на использование этого образа для `airflow-webserver`,
    `airflow-scheduler` и `airflow-init`;
  - обновить документацию (README/TESTING) под новую схему сборки.

- [x] Переключить Airflow с `SequentialExecutor` (SequentialScheduler) на `LocalExecutor`
  для docker‑стенда:
  - проверить, какие параметры достаточно поменять в env/конфиге (`AIRFLOW__CORE__EXECUTOR`)
    для образа `apache/airflow:2.9.2`;
  - убедиться, что примерные DAG'и (`csv_to_greenplum`, `bookings_to_gp_stage`) ведут себя
    предсказуемо в режиме параллельного исполнения;
  - при необходимости скорректировать тесты и документацию (README/TESTING) с учётом нового executor'а.

- [x] Разобрать и стабилизировать интеграцию с Greenplum/PXF:
  - убедиться, что PXF в контейнере `greenplum` всегда корректно инициализируется
    (нет ошибок вида `protocol "pxf" does not exist` при первом запуске `make ddl-gp`);
  - при необходимости доработать init‑скрипты в `pxf/init/` и/или документацию,
    чтобы порядок действий для ментей был однозначным и воспроизводимым;
  - добавить краткий раздел в README/TESTING о типичных ошибках PXF/Greenplum и шагах по их устранению.
  - диагностика текущего кейса: `docs/internal/pxf_bookings.md` (раздел «Известная проблема»).

- [x] Разобраться с генератором demodb:
  - после `make bookings-generate` таблица `bookings.bookings` остаётся пустой;
  - патчи `bookings/patches/engine_jobs1_sync.patch` и `bookings/patches/install_drop_if_exists.patch`
    падают при применении (hunk failed / garbage in patch);
  - из‑за этого DAG `bookings_to_gp_stage` валится на проверках (источник пустой).
  - план: `plans/bookings-demodb-bugfix-plan.md`

- [x] Добавить раздел «Благодарности» в `README.md`:
  - явно поблагодарить Postgres Pro за демо‑БД bookings (репозиторий `postgrespro/demodb`);
  - указать автора Docker‑сборки Greenplum (`woblerr/docker-greenplum`, образ `woblerr/greenplum`);
  - при необходимости сослаться на соответствующие лицензии/README исходных проектов.

- [x] Добавить в образ Airflow установку `psql`, чтобы тестировать загрузку CSV из CLI внутри контейнера (без root и дополнительных зависимостей на хосте).

- [x] Денормализовать `dds.dim_routes` (добавить departure_city, arrival_city, airplane_model, total_seats):
  - привести измерение в соответствие с принципом Кимбалла («самодостаточное измерение»);
  - упростить `dm.route_performance` с 4-JOIN до 1-JOIN;
  - обновить DAG-зависимости: airports+airplanes DQ → routes load;
  - подробный план: `docs/internal/dim_routes_denormalization_plan.md`.
