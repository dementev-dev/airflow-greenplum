# airflow-dwh-gp-lab

Учебный стенд: ETL из Postgres в Greenplum с оркестрацией в Airflow.

Добро пожаловать в учебный стенд для изучения основ Data Engineering! Этот проект поможет вам освоить ключевые инструменты современных data pipeline: **Airflow** для оркестрации, **pandas/CSV** для подготовки данных, **Postgres** с демобазой **bookings** как источник и **Greenplum** как аналитическую базу данных.

Если вы проходите стенд как серию лабораторных, смотрите также файл с заданиями: `educational-tasks.md`.

## 🎯 Что вы узнаете

- Как настроить локальный стек данных с помощью Docker
- Как Airflow управляет workflow и координирует задачи
- Как генерировать датасеты через pandas и сохранять их в CSV
- Как загружать данные в Greenplum пакетами и избегать дублей
- Как проверять качество данных в автоматизированных pipeline
- Основы проектирования ETL/ELT процессов
 - Как работать с демо-БД bookings в Postgres как источником для будущего DWH

## 👩‍🎓 Для студентов (10‑минутный чек‑лист)

- Установите Docker Desktop и Git.
- Скопируйте настройки: `cp .env.example .env`.
- Поднимите стенд: `make up` (или `docker compose up -d` — сервис `airflow-init` запустится автоматически при первом старте).
- Откройте UI: http://localhost:8080 (admin/admin).
- Включите и запустите DAG `csv_to_greenplum`. Дождитесь Success.
- Проверьте данные: `make gp-psql` → `SELECT COUNT(*) FROM public.orders;`.
- Дополнительно: запустите `csv_to_greenplum_dq` — все проверки должны быть зелёные.

Если что‑то не работает — смотрите «Типичные проблемы» и «Быстрый reset» ниже.

## 🚀 Быстрый старт (для новичков)

### Шаг 1: Подготовка окружения

**Требования:**
- Docker Desktop (Windows/Mac) или Docker Engine 24+ (Linux)
- Git для клонирования репозитория

> 💡 **Совет:** Если у вас Windows, рекомендуем использовать WSL (Windows Subsystem for Linux) для лучшей совместимости.

### Шаг 2: Настройка проекта

```bash
# Скопируйте файл настроек
cp .env.example .env

# Запустите стек (это может занять 2-3 минуты при первом запуске)
docker compose up -d
```

### Шаг 3: Первый запуск pipeline

1. Откройте Airflow UI: **http://localhost:8080** (логин/пароль: admin/admin)
2. Найдите DAG с названием **csv_to_greenplum**
3. Нажмите на переключатель слева от названия DAG, чтобы включить его
4. Нажмите кнопку **Trigger** (значок воспроизведения ▶️)

🎉 **Поздравляем!** Вы только что запустили свой первый data pipeline:
- Система сгенерировала 1000 тестовых заказов при помощи pandas
- Датасет сохранился в CSV-файл в каталоге `./data`
- Airflow загрузил данные из CSV в Greenplum без дублей по `order_id`

### Шаг 4: Проверка результатов

**Проверка вручную:**
```bash
# Подключитесь к Greenplum и проверьте данные
docker compose exec greenplum bash -c "su - gpadmin -c 'psql -p 5432 -d gp_dwh'"

# Внутри psql выполните:
\dt                    # Показать таблицы
SELECT count(*) FROM public.orders;  # Посчитать записи

# Посмотреть несколько строк
SELECT * FROM public.orders LIMIT 5;
```

CSV-файлы после выполнения DAG остаются в директории `./data`. Их можно открыть любым редактором или изучить через pandas.

### Быстрый reset

Если после изменений что‑то «сломалось»:

```bash
make down                 # Остановить и удалить контейнеры/сети (volumes сохраняются)
make up
```

Если проблема связана с «грязной» остановкой и данными в томах (например, Greenplum не стартует),
используйте полный reset: `make clean && make up` (данные в Docker-томах будут потеряны).

---

## 🛠️ Подробная настройка (для уверенных пользователей)

> Если вы впервые запускаете стенд, этот раздел можно пролистать и вернуться к нему позже.

### Установка Make (опционально)

Для удобства работы с проектом рекомендуем установить `make`:

- **Linux (Debian/Ubuntu):** `sudo apt install -y make`
- **macOS:** `brew install make`
- **Windows:** 
  - WSL: `sudo apt install -y make`
  - Chocolatey: `choco install make`
  - Scoop: `scoop install make`

С `make` команды становятся короче:
```bash
make up                         # Запуск стека (включая airflow-init при первом старте)
make logs                       # Просмотр логов
make gp-psql                    # Подключение к Greenplum
```

### Настройка подключения к Greenplum в Airflow

По умолчанию готовые DAG используют Airflow Connections. В docker-compose они
заводятся автоматически через переменные окружения:

- `AIRFLOW_CONN_GREENPLUM_CONN` — подключение к Greenplum с `conn_id=greenplum_conn`;
- `AIRFLOW_CONN_BOOKINGS_DB` — подключение к демо-БД bookings с `conn_id=bookings_db`.

Такие подключения подхватываются из окружения и могут не отображаться в UI,
но для DAG это нормально — `PostgresOperator` найдёт их по `conn_id`.

При желании вы можете создать или отредактировать подключение вручную в UI:

1. Airflow UI → **Admin → Connections → Add a new record**
2. Заполните поля:
   - **Conn Id:** `greenplum_conn`
   - **Conn Type:** `Postgres`
   - **Host:** `greenplum`
   - **Schema:** `gp_dwh`
   - **Login:** `gpadmin`
   - **Password:** `gpadmin`
   - **Port:** `5432`

---

### Greenplum + PXF: свой образ

Чтобы избежать проблем с правами и нестабильных запусков, Greenplum собирается
из собственного `Dockerfile.greenplum`. В образ вшиты:

- JDBC-драйвер PostgreSQL;
- конфигурация PXF-сервера `bookings-db`;
- ensure-скрипт, который при каждом старте контейнера докладывает файлы в `PXF_BASE`.

Дополнительно при старте контейнера:

- базовые конфиги PXF копируются в `PXF_BASE/conf` (если их ещё нет);
- создаются каталоги `PXF_BASE/run` и `PXF_BASE/logs`;
- `CREATE EXTENSION pxf` выполняется автоматически, когда Greenplum становится доступен (с ретраями).

Healthcheck сервиса `greenplum` учитывает не только готовность Greenplum, но и запуск PXF,
а также наличие `extension pxf` — это нужно, чтобы Airflow не стартовал раньше PXF.

Сборка и запуск:

- `make build` — собрать образ (явно);
- `make up` — поднимет стек и соберёт образ, если он ещё не создан.

Обновление PXF-конфигов:

- изменили файлы в `pxf/` → выполните `make build` и перезапустите контейнер;
- для принудительной перезаписи файлов в `PXF_BASE` используйте `PXF_SEED_OVERWRITE=1`;
- для принудительного `pxf cluster sync` при старте используйте `PXF_SYNC_ON_START=1`.

Проверка PXF:

- статус: `docker compose exec greenplum bash -lc "su - gpadmin -c '/usr/local/pxf/bin/pxf cluster status'"`;
- логи: `greenplum_data:/data/pxf/logs` (внутри контейнера — `/data/pxf/logs`).

---

### Airflow: свой образ

Airflow тоже собирается из собственного `Dockerfile.airflow`, чтобы зависимости ставились при сборке, а не во время старта контейнеров. В образ включены:

- Python‑зависимости из `airflow/requirements.txt`;
- утилита `psql` для быстрых проверок внутри контейнера.

Если меняли `airflow/requirements.txt` или `Dockerfile.airflow`, пересоберите образы: `make build`, затем `make up`.

В docker-compose по умолчанию задан `AIRFLOW__CORE__EXECUTOR=LocalExecutor` (параллельное выполнение задач). Если нужен последовательный режим — замените на `SequentialExecutor` в `docker-compose.yml`.

---

### Локальное окружение разработчика

Локальным окружением управляет [uv](https://docs.astral.sh/uv/) — он скачивает нужный Python и создаёт `.venv` на основе `pyproject.toml` / `uv.lock`.

```bash
uv sync
```

`uv sync` сам подтянет версию Python из `.python-version`/`pyproject.toml`, создаст `.venv` и установит зависимости. Для тех же действий можно использовать `make dev-sync`. Цель `make dev-setup` (или вручную `uv python install` + `uv python pin`) нужна только когда вы меняете версию Python или прогреваете кэш.

> Если требуется «классическое» активированное окружение, после `uv sync` выполните `.\.venv\Scripts\Activate.ps1` в PowerShell или `source .venv/bin/activate` в Unix-терминале.

Проверки и форматирование выполняем через uv:

```bash
make test            # uv run pytest -q
make lint            # black/isort в режиме проверки
make fmt             # автоформатирование black + isort
```

#### Быстрый старт с uv

```bash
uv sync
uv run pytest -q
uv run black --check airflow tests
```

> Не устанавливайте пакеты напрямую через `pip install --user ...`. Если что-то уже попало в user-site, удалите `pip uninstall <package>` и проверьте `pip list --user`.
---


## 📋 Что входит в стенд

### Основные компоненты
- **Greenplum** — аналитическая база данных для хранения и анализа данных
- **Airflow** — оркестратор workflow и задач
- **Postgres** — база метаданных для Airflow
- **Postgres (bookings)** — отдельная демо-БД bookings (источник данных для будущего DWH в Greenplum)
- **pandas** — библиотека для генерации и анализа данных в формате CSV

### Готовые DAG (workflow)
- **orders_base_ddl** — создаёт базовую таблицу `public.orders` для CSV‑пайплайна
- **bookings_stg_ddl** — готовит схему `stg` и таблицы `stg.bookings_ext` / `stg.bookings`
- **csv_to_greenplum** — базовый pipeline: pandas → CSV → Greenplum
- **bookings_to_gp_stage** — пример загрузки из демо‑БД bookings в слой STG
- **csv_to_greenplum_dq** — проверки качества данных (наличие таблицы, схема, дубликаты)

> Учебный путь — триггернуть DDL‑DAG: для CSV `orders_base_ddl`, для bookings `bookings_stg_ddl`. Технический шорткат для быстрой инициализации — `make ddl-gp` (он не вызывается автоматически при старте контейнеров).

### Полезные команды
```bash
# Основные команды
make up                 # Запустить весь стенд (Airflow инициализируется автоматически при первом старте)
make stop               # Остановить контейнеры, не трогая данные
make down               # Остановить и удалить контейнеры/сети (volumes сохраняются)
make clean              # Полный reset: остановить и удалить контейнеры/сети и тома (данные будут потеряны)
make airflow-init       # Ручной запуск инициализации Airflow (обычно не нужен)
make ddl-gp             # Применить DDL к Greenplum вручную
make gp-psql            # Подключиться к Greenplum через psql
make bookings-init      # Установить демобазу bookings в Postgres (по умолчанию генерирует 1 день)
make bookings-generate-day  # Добавить ещё один день данных в bookings (можно вызвать несколько раз)
make bookings-psql      # Подключиться к демобазе bookings (БД demo)

# Проверка данных
make logs               # Следить за логами Airflow

# Логи задач Airflow сохраняются в Docker-томе `airflow_logs`
# и переживают `docker compose down`/`up` (удаляются при `docker compose down -v` / `make clean`).

# Контроль генерации bookings
docker compose -f docker-compose.yml exec bookings-db bash -lc 'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d demo -c "SELECT busy();"'
# busy() = t — генерация ещё идёт; f — завершена. При необходимости можно вызвать CALL abort(); и запустить генерацию заново.
```

### Генерация следующего дня в bookings

- При `make bookings-init` автоматически генерируется `BOOKINGS_INIT_DAYS` суток, начиная с даты `BOOKINGS_START_DATE` (по умолчанию один день с `2017-01-01`).
- Дальше каждый вызов `make bookings-generate-day` или генерации через DAG добавляет ровно **один** следующий день после `max(book_date)` в `bookings.bookings` — генератор сам смотрит последнюю дату.
- Рекомендуемый учебный сценарий для DAG `bookings_to_gp_stage`: запускать DAG по одному дню вперёд, выбирая в форме Trigger логическую дату `Execution Date (ds)`, совпадающую с тем днём, который вы хотите загрузить (например, `2017-01-01`, затем `2017-01-02` и т.д.).

- Быстрее всего: `make bookings-generate-day` — читает GUC и сам вызывает `continue`.
- Вручную из psql/DBeaver (подзапросы в аргументах CALL не работают, поэтому через DO-блок):
  ```sql
  DO $$
  DECLARE
    v_next_day timestamptz;
  BEGIN
    SELECT date_trunc('day', max(book_date)) + interval '1 day'
    INTO v_next_day
    FROM bookings.bookings;
    CALL continue(v_next_day);           -- или CALL continue(v_next_day, 4) для параллельности
  END $$;
  ```
- Не вызывайте `CALL generate(...)` поверх существующих данных: она делает TRUNCATE и создаёт демобазу заново.

---

## ⚙️ Настройка через переменные окружения

Все настройки находятся в файле `.env`. Основные параметры:

### Greenplum
- `GP_USER` — пользователь (по умолчанию: gpadmin)
- `GP_PASSWORD` — пароль (по умолчанию: gpadmin)
- `GP_DB` — база данных (по умолчанию: gp_dwh)
- `GP_PORT` — порт Greenplum внутри Docker-сети (по умолчанию: 5432, менять обычно не нужно; внешний порт на хосте для подключения клиентов — 5435).

### Демо-БД bookings (Postgres)
- `BOOKINGS_DB_USER` — пользователь Postgres для демобазы (по умолчанию: bookings)
- `BOOKINGS_DB_PASSWORD` — пароль пользователя (по умолчанию: bookings)
- `BOOKINGS_DB_NAME` — база данных, из которой запускается установка генератора (по умолчанию: bookings)
- `BOOKINGS_DB_PORT` — внешний порт для подключения к контейнеру bookings-db (по умолчанию: 5434)
- `BOOKINGS_START_DATE` — начальная дата модельного времени (по умолчанию: 2017-01-01)
- `BOOKINGS_INIT_DAYS` — сколько дней сгенерировать при первой инициализации (по умолчанию: 1, чтобы увидеть данные без долгого ожидания; можно менять при вызове `BOOKINGS_INIT_DAYS=... make bookings-init`)
- `BOOKINGS_JOBS` — число параллельных джобов генератора bookings (по умолчанию: 1; при 1 генерация идёт синхронно без dblink)

### CSV pipeline
- `CSV_DIR` — путь к каталогу с CSV внутри контейнеров Airflow (по умолчанию: `/opt/airflow/data`)
- `CSV_ROWS` — количество строк, генерируемых DAG (по умолчанию: 1000)

### Airflow
- `GP_CONN_ID` — ID подключения (по умолчанию: greenplum_conn)

---

## 🔍 Продвинутые темы

> Этот раздел не обязателен при первом прохождении стенда; к нему удобно вернуться, когда базовый CSV‑pipeline уже понятен.

### Архитектура pipeline

**Поток данных в DAG `csv_to_greenplum`:**
1. `create_orders_table` — создаёт таблицу `public.orders` в Greenplum
2. `generate_csv` — генерирует датасет при помощи pandas и сохраняет CSV в `CSV_DIR`
3. `preview_csv` — выводит предпросмотр и статистику по данным
4. `load_csv_to_greenplum` — загружает CSV во временную таблицу и переносит новые строки в `public.orders`

> 💡 **Безопасность повторного запуска:** Pipeline защищен от дубликатов, поэтому его можно запускать многократно.

### Проверка качества данных

Запустите DAG `csv_to_greenplum_dq` для автоматической проверки:
- Наличие таблицы в базе
- Соответствие схемы ожидаемой структуре
- Объем загруженных данных
- Отсутствие дубликатов записей

### Пример DAG с SQL-скриптами (bookings → stg)

> Если вы ещё не дошли до части про bookings и слои DWH, этот подраздел можно пропустить на первом чтении.

В репозитории есть учебный DAG `bookings_to_gp_stage`, который показывает «канонический» способ работы с SQL в Airflow:

- подключение к БД через Airflow Connections (`bookings_db`, `greenplum_conn`);
- бизнес-логика инкрементальной загрузки и DQ вынесена в SQL-файлы в каталоге `sql/`:
  - `sql/src/bookings_generate_day_if_missing.sql` — генерация следующего учебного дня в демо-БД bookings (или нескольких стартовых дней, если база пуста);
  - `sql/stg/bookings_ddl.sql` — DDL для схемы `stg` и таблиц `stg.bookings_ext` / `stg.bookings`;
  - `sql/stg/bookings_load.sql` — загрузка инкремента из `stg.bookings_ext` в `stg.bookings` на основе «хвоста» после предыдущих батчей;
  - `sql/stg/bookings_dq.sql` — проверка количества строк между источником и stg за то же окно.

Фрагмент DAG:

```python
load_bookings_to_stg = PostgresOperator(
    task_id="load_bookings_to_stg",
    postgres_conn_id="greenplum_conn",
    sql="stg/bookings_load.sql",
    params={"batch_id": "{{ run_id }}"},
)
```

Такой подход помогает держать оркестрацию (DAG) и SQL-логику в отдельных файлах и легче сравнивать её с теорией из статьи про моделирование DWH.

### Ограничения учебного стенда

- **Greenplum** запущен в single-node режиме (для обучения)
- В продакшене Greenplum обычно разворачивают кластером на нескольких серверах
- Используется Greenplum 6 (широко доступная версия), хотя Greenplum 7 предлагает больше возможностей

---

## 🧩 Подключение к базам через DBeaver

> Необязательный раздел: нужен только если вы хотите смотреть данные через DBeaver. Для базовых заданий достаточно `make gp-psql`.

Ниже — краткая инструкция, как подключиться к Greenplum и демо-БД bookings из DBeaver. Перед этим убедитесь, что стенд запущен:

- `cp .env.example .env` (если ещё не делали)
- `make up`
- `make bookings-init`
- `make ddl-gp`

### Greenplum (аналитическая БД)

1. Откройте DBeaver → **New Database Connection**.
2. Выберите драйвер **PostgreSQL** (или **Greenplum**, если он есть в вашей версии DBeaver).
3. На вкладке **Main** заполните поля (по умолчанию):
   - `Host`: `localhost`
   - `Port`: `5435` (внешний порт Greenplum на хосте)
   - `Database`: значение `GP_DB` (по умолчанию `gp_dwh`)
   - `Username`: значение `GP_USER` (по умолчанию `gpadmin`)
   - `Password`: значение `GP_PASSWORD` (по умолчанию `gpadmin`)
4. Нажмите **Test Connection** → **OK**, затем **Finish**.

После подключения:

- Основные таблицы лаба — в схеме `public` базы `gp_dwh` (например, `public.orders`).
- После настройки PXF и выполнения `make ddl-gp` станет доступна внешняя таблица `public.ext_bookings_bookings` — чтение из демо-БД bookings через PXF.

### bookings-db (демо-БД источника)

Для работы с исходными данными (демо-БД `demo`) достаточно стандартного PostgreSQL-подключения.

1. Откройте DBeaver → **New Database Connection** → драйвер **PostgreSQL**.
2. На вкладке **Main** укажите (значения по умолчанию из `.env.example`):
   - `Host`: `localhost`
   - `Port`: значение `BOOKINGS_DB_PORT` из `.env` (по умолчанию `5434`)
   - `Database`: `demo`
   - `Username`: `BOOKINGS_DB_USER` (по умолчанию `bookings`)
   - `Password`: `BOOKINGS_DB_PASSWORD` (по умолчанию `bookings`)
3. Нажмите **Test Connection** → **OK**, затем **Finish**.

После подключения:

- Основные таблицы находятся в схеме `bookings` базы `demo` (например, `bookings.bookings`, `bookings.tickets`, `bookings.flights` и т.д.).
- Можно сравнивать данные:
  - между `bookings.bookings` в Postgres и `public.ext_bookings_bookings` в Greenplum;
  - между временем (`book_date`) в UTC в `demo` и локальным временем в Greenplum (учитывая `TZ=Europe/Moscow`).

> Если вы меняли порты или креды в `.env`, не забудьте подставить те же значения в настройках соединений в DBeaver.

---

## 🆘 Типичные проблемы и решения

| Проблема | Решение |
|----------|---------|
| Airflow UI не открывается | Дождитесь сообщения `Listening at: http://0.0.0.0:8080` в логах (`make logs`) |
| `database "demo" does not exist` в bookings‑DAG | Вы сделали полный reset с удалением томов (`docker compose down -v` / `make clean`), поэтому демобаза bookings не установлена. Запустите `make bookings-init` и повторите DAG. |
| Ошибка подключения к Greenplum | Убедитесь, что контейнер `greenplum` стал статусом `healthy` (проверьте `docker compose ps`) |
| Не открывается порт 8080/5433/5434/5435 | Проверьте, что эти порты не заняты локальными сервисами; при необходимости остановите их или измените порты в `.env`/`docker-compose.yml` |
| Нет файла в `./data` после запуска DAG | Проверьте логи задачи `generate_csv`, убедитесь, что `CSV_DIR` смонтирован в docker-compose |
| Команда `make` не найдена | Используйте полные команды `docker compose` или установите make |
| Greenplum не стартует/падает при старте | Попробуйте `make down && make up`. Если не помогло — полный reset: `make clean && make up` (удалит тома). |
| `protocol "pxf" does not exist` | Перезапустите `greenplum` и повторите `bookings_stg_ddl`/`make ddl-gp` — расширение `pxf` создаётся автоматически при старте контейнера. |
| PXF не отвечает (Connection refused к порту 5888) | Проверьте `pxf cluster status` в контейнере `greenplum` и перезапустите сервис `greenplum`. |
| PXF не подхватывает изменения конфигов | Пересоберите образ (`make build`) и перезапустите `greenplum`. Для принудительной перезаписи файлов задайте `PXF_SEED_OVERWRITE=1`. |
| DAG `bookings_to_gp_stage` падает на внешней таблице/подключении к bookings | Убедитесь, что запущен контейнер `bookings-db` (`docker compose ps`, при необходимости `docker compose start bookings-db`), и выполнены `make bookings-init` и `make ddl-gp` или DAG `bookings_stg_ddl` |
| DAG `bookings_to_gp_stage` ругается на отсутствующие таблицы stg | Запустите DAG `bookings_stg_ddl` (или выполните `make ddl-gp`), затем повторите запуск |
| DAG не видит Greenplum/DEMObase по Airflow Connections | Убедитесь, что контейнеры `greenplum` и `bookings-db` запущены (`docker compose ps`). Подключения `greenplum_conn` и `bookings_db` задаются через переменные окружения `AIRFLOW_CONN_...` и могут не отображаться в UI, но `PostgresOperator` всё равно найдёт их по `conn_id`. При необходимости вы можете создать/отредактировать их вручную в разделе Connections. |

---

## 📁 Структура проекта

```
├── docker-compose.yml      # Описание всех сервисов
├── Dockerfile.airflow      # Образ Airflow с зависимостями
├── Dockerfile.greenplum    # Образ Greenplum с интегрированным PXF
├── .env.example            # Шаблон настроек
├── Makefile                # Удобные команды для работы
├── README.md               # Обзор стенда
├── TESTING.md              # Пошаговый план проверки
├── educational-tasks.md    # Учебные задания для менти
├── airflow/
│   └── dags/               # Файлы workflow (DAG)
│       ├── csv_to_greenplum.py
│       ├── csv_to_greenplum_dq.py
│       └── bookings_to_gp_stage.py
├── bookings/               # Скрипты и файлы для демобазы bookings в Postgres
├── sql/
│   └── ddl_gp.sql          # Общий DDL для Greenplum (подключает stg/src-скрипты)
├── docs/                   # Дополнительные документы (архитектура, bookings/STG, PXF)
├── tests/                  # Автоматические тесты (pytest)
└── pxf/                    # Конфигурация и файлы для PXF
```

---

## 💡 Советы для дальнейшего обучения

1. **Поэкспериментируйте с DAG** — измените параметры генерации данных или размер батча
2. **Добавьте свои проверки** — расширьте DAG `csv_to_greenplum_dq.py`
3. **Попробуйте другие источники** — замените генератор данных на чтение из файла или API
4. **Изучите Airflow deeper** — добавьте зависимости между задачами, настройте расписания

---

## ✅ Тестирование

- Локальные проверки: `make test` (pytest). Для форматирования — `make fmt`, для проверки — `make lint`.
- Пошаговый сценарий с Docker (включая негативные кейсы и reset) — см. `TESTING.md`.
- Полный smoke-тест стенда (сносит volumes!): `make e2e-smoke` — поднимает стек с нуля, прогоняет `csv_to_greenplum` и `bookings_to_gp_stage` через `airflow dags test` и проверяет, что в `public.orders` и `stg.bookings` появились строки.


---

## Благодарности

- **Postgres Pro** — за демо-БД bookings и генератор данных `demodb`: https://github.com/postgrespro/demodb (лицензия MIT: https://github.com/postgrespro/demodb/blob/main/LICENSE).
- **woblerr** — за Docker-сборку Greenplum: https://github.com/woblerr/docker-greenplum (образ: `woblerr/greenplum`, лицензия MIT: https://github.com/woblerr/docker-greenplum/blob/master/LICENSE).

Удачи в изучении Data Engineering! 🚀
