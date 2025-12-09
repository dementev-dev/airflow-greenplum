# airflow-dwh-gp-lab

Учебный стенд: ETL из Postgres в Greenplum с оркестрацией в Airflow.

Добро пожаловать в учебный стенд для изучения основ Data Engineering! Этот проект поможет вам освоить ключевые инструменты современных data pipeline: **Airflow** для оркестрации, **pandas/CSV** для подготовки данных, **Postgres** с демобазой **bookings** как источник и **Greenplum** как аналитическую базу данных.

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
- Поднимите стенд: `docker compose up -d` и инициализируйте Airflow: `docker compose run --rm airflow-init`.
- Откройте UI: http://localhost:8080 (admin/admin).
- Включите и запустите DAG `csv_to_greenplum`. Дождитесь Success.
- Проверьте данные: `make gp-psql` → `SELECT COUNT(*) FROM public.orders;`.
- Дополнительно: запустите `greenplum_data_quality` — все проверки должны быть зелёные.

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
docker compose exec greenplum bash -c "su - gpadmin -c 'psql -p 5432 -d gpadmin'"

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
make down                 # Остановить и стереть данные в контейнерах
make up
```

Это помогает, когда Greenplum не стартует из‑за «грязной» остановки и внутренних файлов.

---

## 🛠️ Подробная настройка (для уверенных пользователей)

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
make up && make airflow-init    # Запуск стека
make logs                       # Просмотр логов
make gp-psql                    # Подключение к Greenplum
```

### Настройка подключения к Greenplum в Airflow

По умолчанию DAG использует переменные окружения, но вы можете создать Airflow Connection:

1. Airflow UI → **Admin → Connections → Add a new record**
2. Заполните поля:
   - **Conn Id:** `greenplum_conn`
   - **Conn Type:** `Postgres`
   - **Host:** `greenplum`
   - **Schema:** `gpadmin`
   - **Login:** `gpadmin`
   - **Password:** `gpadmin`
   - **Port:** `5432`

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
- **csv_to_greenplum** — базовый pipeline: pandas → CSV → Greenplum
- **greenplum_data_quality** — проверки качества данных (наличие таблицы, схема, дубликаты)

### Полезные команды
```bash
# Основные команды
make up                 # Запустить весь стенд
make down               # Остановить и удалить данные
make airflow-init       # Инициализировать Airflow
make ddl-gp             # Применить DDL к Greenplum
make gp-psql            # Подключиться к Greenplum через psql
make bookings-init      # Установить демобазу bookings в Postgres (по умолчанию генерирует 1 день)
make bookings-generate-day  # Добавить ещё один день данных в bookings (можно вызвать несколько раз)
make bookings-psql      # Подключиться к демобазе bookings (БД demo)

# Проверка данных
make logs               # Следить за логами Airflow

# Контроль генерации bookings
docker compose -f docker-compose.yml exec bookings-db bash -lc 'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d demo -c "SELECT busy();"'
# busy() = t — генерация ещё идёт; f — завершена. При необходимости можно вызвать CALL abort(); и запустить генерацию заново.
```

---

## ⚙️ Настройка через переменные окружения

Все настройки находятся в файле `.env`. Основные параметры:

### Greenplum
- `GP_USER` — пользователь (по умолчанию: gpadmin)
- `GP_PASSWORD` — пароль (по умолчанию: gpadmin)
- `GP_DB` — база данных (по умолчанию: gpadmin)
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

### Архитектура pipeline

**Поток данных в DAG `csv_to_greenplum`:**
1. `create_orders_table` — создаёт таблицу `public.orders` в Greenplum
2. `generate_csv` — генерирует датасет при помощи pandas и сохраняет CSV в `CSV_DIR`
3. `preview_csv` — выводит предпросмотр и статистику по данным
4. `load_csv_to_greenplum` — загружает CSV во временную таблицу и переносит новые строки в `public.orders`

> 💡 **Безопасность повторного запуска:** Pipeline защищен от дубликатов, поэтому его можно запускать многократно.

### Проверка качества данных

Запустите DAG `greenplum_data_quality` для автоматической проверки:
- Наличие таблицы в базе
- Соответствие схемы ожидаемой структуре
- Объем загруженных данных
- Отсутствие дубликатов записей

### Ограничения учебного стенда

- **Greenplum** запущен в single-node режиме (для обучения)
- В продакшене Greenplum обычно разворачивают кластером на нескольких серверах
- Используется Greenplum 6 (широко доступная версия), хотя Greenplum 7 предлагает больше возможностей

---

## 🧩 Подключение к базам через DBeaver

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
   - `Database`: значение `GP_DB` (по умолчанию `gpadmin`)
   - `Username`: значение `GP_USER` (по умолчанию `gpadmin`)
   - `Password`: значение `GP_PASSWORD` (по умолчанию `gpadmin`)
4. Нажмите **Test Connection** → **OK**, затем **Finish**.

После подключения:

- Основные таблицы лаба — в схеме `public` базы `gpadmin` (например, `public.orders`).
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
| Ошибка подключения к Greenplum | Убедитесь, что контейнер `greenplum` стал статусом `healthy` (проверьте `docker compose ps`) |
| Нет файла в `./data` после запуска DAG | Проверьте логи задачи `generate_csv`, убедитесь, что `CSV_DIR` смонтирован в docker-compose |
| Команда `make` не найдена | Используйте полные команды `docker compose` или установите make |
| Greenplum не стартует/падает при старте | Выполните `make down`, затем `make up && make airflow-init` (очищает тома и поднимает заново) |

---

## 📁 Структура проекта

```
├── docker-compose.yml     # Описание всех сервисов
├── .env.example           # Шаблон настроек
├── Makefile               # Удобные команды для работы
├── airflow/
│   └── dags/              # Файлы workflow (DAG)
│       ├── csv_to_greenplum.py
│       └── data_quality_greenplum.py
├── bookings/              # Скрипты и вспомогательные файлы для демобазы bookings в Postgres
└── sql/
    └── ddl_gp.sql         # Создание таблицы в Greenplum
```

---

## 💡 Советы для дальнейшего обучения

1. **Поэкспериментируйте с DAG** — измените параметры генерации данных или размер батча
2. **Добавьте свои проверки** — расширьте DAG `data_quality_greenplum.py`
3. **Попробуйте другие источники** — замените генератор данных на чтение из файла или API
4. **Изучите Airflow deeper** — добавьте зависимости между задачами, настройте расписания

---

## ✅ Тестирование

- Локальные проверки: `make test` (pytest). Для форматирования — `make fmt`, для проверки — `make lint`.
- Пошаговый сценарий с Docker (включая негативные кейсы и reset) — см. `TESTING.md`.

Удачи в изучении Data Engineering! 🚀
