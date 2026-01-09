# План тестирования (для студентов)

Этот документ — пошаговый чек‑лист, как проверить, что всё работает: от «быстрых локальных проверок» до запуска стенда в Docker и просмотра данных в Greenplum. Подходит начинающим: просто выполняйте шаги по порядку.

Если что‑то пошло не так, смотрите раздел «Быстрый reset» ниже.

## 1. Быстрая проверка окружения
- `uv sync` — подтягиваем Python и зависимости из `pyproject.toml`/`uv.lock`.
- Проверяем версию uv: `uv --version` (ожидаем ≥ 0.9).
- Убедитесь, что `docker compose version` доступна и Docker запущен.

## 2. Локальные автоматические проверки (без Docker)
- `make test` — короткие unit-тесты (`tests/test_greenplum_helpers.py`, `tests/test_dags_smoke.py`).
  - Smoke-тесты DAG автоматически `skip`, если Airflow не установлен в venv, поэтому прогонится за миллисекунды.
- `make lint` — black/isort в режиме проверки (после `make fmt` должен проходить без ошибок).
- `make fmt` — автоисправление форматирования; полезно запускать перед пушем.
- (опционально) `uv run pytest -q -k dags_smoke` — только DAG smoke.

## 3. Подготовка Docker-стенда
- `cp .env.example .env` (если файла ещё нет) и проверьте переменные:
  - `GP_PORT` — внутренний порт Greenplum в Docker-сети (по умолчанию 5432, менять не нужно); внешний порт для подключения с хоста фиксирован на `5435`, поэтому локальный PostgreSQL на 5432 не помешает.
  - `GP_USE_AIRFLOW_CONN=true` при желании использовать Airflow Connection; `false` — fallback на ENV.
- `make up` — поднимаем все сервисы. Важно дождаться статуса `healthy` у `pgmeta` и `greenplum` (`docker compose ps`); `greenplum` считается `healthy` только когда поднят и Greenplum, и PXF.
- `make logs` — следим, пока webserver и scheduler не перейдут в рабочее состояние (`Listening at: http://0.0.0.0:8080`).

## 4. Smoke тесты DAG в Airflow UI
1. Открыть http://localhost:8080 (admin/admin).
2. (опционально) Зайти в Admin → Connections и убедиться, что DAG’и видят подключения:
   - `greenplum_conn` и `bookings_db` задаются через переменные `AIRFLOW_CONN_...` в docker-compose и могут не отображаться в списке, но `airflow connections get greenplum_conn` / `bookings_db` внутри контейнера должны отрабатывать без ошибок.
3. DAG `csv_to_greenplum`:
   - Включить переключатель.
   - Нажать «Trigger DAG».
   - Контроль: все таски Success, в `data/` появился CSV, в логах `load_csv_to_greenplum` видно `INSERT`.
   - В Greenplum (см. п.5) убедиться в наличии строк `(SELECT COUNT(*) ...)`.
4. DAG `csv_to_greenplum_dq`:
   - Запустить вручную после первого DAG.
   - Проверить, что все 5 задач Success и логи содержат `Проверка пройдена`.

- DAG `bookings_to_gp_stage` (полная проверка цепочки bookings → Greenplum STG):
  - предварительно выполнить один раз: `make bookings-init` (инициализация демо‑БД bookings) и `make ddl-gp` (создаёт `stg.bookings_ext` и `stg.bookings` в Greenplum);
  - включить DAG `bookings_to_gp_stage` и запустить `Trigger DAG`;
  - убедиться, что все задачи (`generate_bookings_day`, `load_bookings_to_stg`, `check_row_counts`, `finish_summary`) завершились со статусом Success;
  - при желании проверить данные: в `bookings-db` появился новый день, а в Greenplum в `stg.bookings` — строки с актуальным `batch_id` (см. пример запросов в разделе 5).

- (опционально, для менторов/разработчиков) Smoke-тест DAG через Airflow CLI без UI:
   - `docker compose -f docker-compose.yml exec airflow-webserver airflow dags test bookings_to_gp_stage 2024-01-01` — прогоняет `bookings_to_gp_stage` целиком в «off-line» режиме;
   - `docker compose -f docker-compose.yml exec airflow-webserver airflow dags trigger bookings_to_gp_stage` — создаёт реальный запуск DAG (логи и статус можно смотреть либо через UI, либо командой `airflow tasks list`/`airflow tasks logs` внутри контейнера).

## 5. Проверка данных в Greenplum
- `make gp-psql` — запустить psql в контейнере от имени `gpadmin`.
- (опционально) Проверить, что PXF действительно запущен:
  - `docker compose exec greenplum bash -lc "su - gpadmin -c '/usr/local/pxf/bin/pxf cluster status'"`
- Команды внутри psql:
  - `\dt public.*` — таблицы схему public.
  - `SELECT COUNT(*) FROM public.orders;` — оценка объёма.
  - `SELECT * FROM public.orders LIMIT 5;` — визуальная проверка.
  - `SELECT order_id FROM public.orders GROUP BY 1 HAVING COUNT(*) > 1;` — поиск дублей.
  - (после настройки PXF) `SELECT COUNT(*) FROM public.ext_bookings_bookings;` — проверка чтения из демо-БД bookings через PXF.
  - (после настройки PXF) `SELECT * FROM public.ext_bookings_bookings LIMIT 5;` — визуальное сравнение с таблицей `bookings.bookings` в исходной БД.
- Завершить `\q`.

## 6. Негативные сценарии и fallback
- **Пустая таблица**: запустить `csv_to_greenplum_dq` до `csv_to_greenplum`. Ожидается ошибка на таске `check_orders_has_rows`.
- **Проблемы с подключением**: временно изменить `GP_HOST` или `GP_PORT` на несуществующий, перезапустить `make up`, убедиться, что DAG падает с понятной ошибкой (`psycopg2.OperationalError`).
- **Fallback без Airflow Connection**: установить `GP_USE_AIRFLOW_CONN=false`, перезапустить стек (`make down && make up`), удостовериться, что загрузка и DQ работают через ENV.
- **Дубликаты**: дважды вызвать `csv_to_greenplum` — ожидаем, что количество строк в `public.orders` не увеличится на размер CSV, а DAG `csv_to_greenplum_dq` не найдёт дублей.
- **PXF и демобаза bookings** (после настройки PXF и выполнения `make ddl-gp`): временно остановить `bookings-db` (`docker compose stop bookings-db`) и попробовать выполнить `SELECT COUNT(*) FROM public.ext_bookings_bookings;` в `make gp-psql` — ожидается ошибка подключения. Затем запустить `bookings-db` (`docker compose start bookings-db`) и убедиться, что запрос снова работает.

## 7. Быстрый reset (если «что-то сломалось»)
- Перезапустить стенд:
  - Мягкий вариант (сохранить данные): `make stop`, затем `make up`.
  - Полный reset (очистить данные в Docker-томах): `make clean`, затем `make up` (Greenplum/Airflow/bookings будут подняты и инициализированы с нуля).
- Иногда Greenplum не стартует после «грязных» остановок (из‑за старых внутренних файлов). Лечение: всегда делайте `make down` перед повторным `make up`.

## 8. Снятие метрик и мониторинг
- Контейнеры: `docker compose ps`, `docker stats` (по желанию).
- Логи задач: в Airflow UI → конкретный таск → Log.
- Хостовые CSV: каталог `data/` (можно открыть любой файл и убедиться в структуре).

## 9. Завершение работы
- `make down` — выключает сервисы и удаляет тома (перезапишет данные в Greenplum!).
- При необходимости сохранить данные: скопировать CSV из `data/` и дампы из контейнера до `make down`.

## Текущий статус (пример успешного прогона)
- `uv run pytest -q` — 11 passed, 2 smoke-теста DAG пропущены (Airflow не установлен в venv).
- `make lint` — проходит (DAG‑файлы отформатированы black/isort).
- Docker-стенд не запускался в рамках этой сессии; ожидается, что инструкции выше обеспечат полноценную проверку.
