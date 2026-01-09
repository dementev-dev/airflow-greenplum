# Технические детали стенда (Docker / Airflow / Greenplum)

Этот документ не обязателен для прохождения лабораторных, но полезен, если вы:

- хотите понять, как устроен стенд “под капотом”;
- меняете конфиги/зависимости и пересобираете образы;
- отлаживаете проблемы со стартом Greenplum/PXF или Connections в Airflow.

## Make (опционально, но удобно)

Если `make` не установлен, можно пользоваться `docker compose ...`.
Но с `make` команды короче и проще.

Установка `make` (если нужно):

- Linux (Debian/Ubuntu): `sudo apt install -y make`
- macOS: `brew install make`
- Windows:
  - WSL: `sudo apt install -y make`
  - Chocolatey: `choco install make`
  - Scoop: `scoop install make`

Примеры:

```bash
make up
make logs
make gp-psql
```

## Airflow Connections

Готовые DAG по умолчанию подключаются к БД через Airflow Connections:

- `greenplum_conn` — Greenplum;
- `bookings_db` — демо‑БД bookings (`bookings-db`).

В `docker-compose.yml` Connections задаются через переменные окружения `AIRFLOW_CONN_...`,
поэтому они могут не отображаться в UI — для DAG это нормально.

Если нужно завести вручную:

1) Airflow UI → Admin → Connections → Add a new record
2) Пример для Greenplum:
   - Conn Id: `greenplum_conn`
   - Conn Type: `Postgres`
   - Host: `greenplum`
   - Schema: `gp_dwh`
   - Login/Password: `gpadmin` / `gpadmin`
   - Port: `5432`

## PXF (в 5 строк)

PXF (Platform Extension Framework) — компонент Greenplum для работы с внешними источниками.
В этом стенде PXF используется для чтения таблицы `bookings.bookings` из Postgres прямо из Greenplum
через внешнюю таблицу `stg.bookings_ext`. Поэтому загрузка в `stg.bookings` выглядит как обычный
`INSERT ... SELECT` без промежуточных CSV.

## Greenplum + PXF: свой образ

Greenplum собирается из собственного `Dockerfile.greenplum`, чтобы:

- не ловить проблемы с правами/`chown` при bind‑mount конфигов;
- держать PXF‑конфиги и JDBC‑драйвер внутри образа;
- делать старт контейнера идемпотентным.

При старте контейнера:

- seed‑конфиги PXF докладываются в `PXF_BASE` на persistent volume;
- создаются каталоги `PXF_BASE/run` и `PXF_BASE/logs`;
- расширение `pxf` создаётся автоматически, когда Greenplum становится доступен (с ретраями).

Полезные команды:

```bash
make build              # пересобрать образы
docker compose ps       # проверить health
```

Перезапись seed‑файлов в `PXF_BASE`:

- `PXF_SEED_OVERWRITE=1` — перезаписать конфиги при старте;
- `PXF_SYNC_ON_START=1` — выполнить `pxf cluster sync` при старте.

Подробнее: `docs/internal/pxf_bookings.md`.

## Airflow: свой образ

Airflow тоже собирается из собственного `Dockerfile.airflow`, чтобы зависимости ставились при сборке,
а не во время старта контейнеров.

Если меняли `airflow/requirements.txt` или `Dockerfile.airflow`, пересоберите образы:

```bash
make build
make up
```

## Локальное окружение разработчика (uv)

Локальным окружением управляет `uv` — он создаёт `.venv` из `pyproject.toml` / `uv.lock`.

```bash
uv sync
make test
make lint
make fmt
```

Не устанавливайте зависимости через `pip install --user ...` — это часто ломает окружение и IDE.

## Переменные окружения (`.env`)

Все настройки лежат в `.env` (шаблон: `.env.example`).

### Порты на хосте

- `AIRFLOW_WEB_PORT` — Airflow UI (по умолчанию `8080`)
- `PGMETA_PORT` — мета‑БД Airflow (по умолчанию `5433`, обычно нужно только для диагностики)
- `BOOKINGS_DB_PORT` — bookings-db (по умолчанию `5434`)
- `GP_HOST_PORT` — Greenplum (по умолчанию `5435`)

### Greenplum

- `GP_USER`, `GP_PASSWORD`, `GP_DB`
- `GP_PORT` — внутренний порт в Docker-сети (обычно `5432`)
- `GP_HOST_PORT` — внешний порт на хосте (по умолчанию `5435`)

### bookings-db (Postgres)

- `BOOKINGS_DB_USER`, `BOOKINGS_DB_PASSWORD`
- `BOOKINGS_DB_PORT` — внешний порт (по умолчанию `5434`)
- `BOOKINGS_START_DATE` — стартовая дата модельного времени
- `BOOKINGS_INIT_DAYS` — сколько дней генерировать при первом `make bookings-init`
- `BOOKINGS_JOBS` — число джобов генератора (по умолчанию `1`)

### CSV pipeline (побочный пример)

- `CSV_DIR` — путь к каталогу с CSV внутри контейнеров Airflow (по умолчанию `/opt/airflow/data`)
- `CSV_ROWS` — количество строк, генерируемых DAG (по умолчанию `1000`)
