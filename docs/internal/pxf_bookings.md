# PXF для bookings в учебном стенде (актуально)

Этот документ описывает **текущую реализацию** PXF в проекте: чтение данных из демо‑БД
`bookings` (Postgres, сервис `bookings-db`) в Greenplum через JDBC.

## 1. Что должно работать

- В Greenplum доступны внешние таблицы:
  - `public.ext_bookings_bookings` (создаётся `make ddl-gp`);
  - `stg.bookings_ext` (создаётся DAG `bookings_stg_ddl`).
- PXF должен быть готов **после каждого старта** контейнера `greenplum`.

## 2. Почему мы делаем свой образ Greenplum

Изначально PXF‑скрипты/конфиги монтировались в контейнер как bind‑mount `:ro`.
Базовый entrypoint образа Greenplum пытается делать `chown` файлов в
`/docker-entrypoint-initdb.d/`, из‑за чего контейнер иногда падал с ошибкой:

`chown: changing ownership ... Read-only file system`

Снять `:ro` тоже нежелательно — можно получить проблемы с правами на файлах хоста
(файл становится `root`, IDE перестаёт сохранять, появляются лишние изменения в git).

Решение для учебного стенда:

- собрать **свой образ** Greenplum (`Dockerfile.greenplum`);
- «вшить» в образ seed‑файлы и скрипты PXF;
- на каждом старте контейнера идемпотентно докладывать файлы в `PXF_BASE`,
  который живёт на persistent volume.

## 3. Где что хранится

**Внутри образа (immutable):**

- seed для PXF: `/opt/pxf-seed/` (JDBC‑JAR и `servers/bookings-db/jdbc-site.xml`);
- скрипты:
  - `/opt/pxf-scripts/ensure_pxf_bookings.sh` (подготовка `PXF_BASE`);
  - `/start_greenplum_with_pxf.sh` (startup wrapper).

**На persistent volume (переживает рестарты):**

- `PXF_BASE` по умолчанию: `${GREENPLUM_DATA_DIRECTORY}/pxf` → в нашем compose это
  `/data/pxf` на томе `greenplum_data`.

Важно: так как `PXF_BASE` лежит на томе, обновления seed‑файлов из нового образа
**не перезатирают** файлы в `PXF_BASE` автоматически (это сделано намеренно, чтобы
не ломать ручные правки студентов).

## 4. Что происходит при старте контейнера `greenplum`

1) Docker запускает контейнер с базовым entrypoint образа и командой
`/start_greenplum_with_pxf.sh` (она задана в `Dockerfile.greenplum` как `CMD`).

2) `/start_greenplum_with_pxf.sh` выполняет подготовку PXF:

- запускает ensure‑скрипт `/opt/pxf-scripts/ensure_pxf_bookings.sh`;
- параллельно пытается выполнить `CREATE EXTENSION IF NOT EXISTS pxf`
  в базе `${GP_DB}` (по умолчанию `gp_dwh`), когда Greenplum начинает принимать
  подключения.

3) Затем управление передаётся оригинальному старту Greenplum: `exec /start_gpdb.sh`.

4) Healthcheck сервиса `greenplum` ждёт и готовность Greenplum, и то, что PXF уже
запущен (`pxf cluster status`). Это нужно, чтобы Airflow не стартовал раньше PXF.

## 5. Управляющие переменные окружения

Все переменные можно задать в `.env` (см. `.env.example`):

- `PXF_SEED_OVERWRITE=1` — принудительно перезаписать seed‑файлы из образа в `PXF_BASE`
  (обычно нужно после правок в каталоге `pxf/`).
- `PXF_SYNC_ON_START=1` — выполнять `pxf cluster sync` при старте контейнера
  (делает старт чуть дольше, но гарантирует актуальные конфиги на хостах кластера).

## 6. Быстрая ручная проверка

1) Дождаться `healthy` у `greenplum`:

`docker compose ps`

2) Проверить статус PXF (PXF CLI запускается только под пользователем `gpadmin`):

`docker compose exec greenplum bash -lc "su - gpadmin -c '/usr/local/pxf/bin/pxf cluster status'"`

3) После применения DDL (`make ddl-gp`) проверить чтение через PXF:

- `make gp-psql`
- `SELECT COUNT(*) FROM public.ext_bookings_bookings;`

## 7. Типовые ошибки

- `protocol "pxf" does not exist`
  - причина: не создано расширение `pxf` в базе Greenplum;
  - решение: перезапустить `greenplum` (скрипт сделает `CREATE EXTENSION IF NOT EXISTS pxf`)
    или выполнить вручную `CREATE EXTENSION pxf;`.
- `Connection refused` к порту `5888`
  - причина: PXF не поднялся/не успел подняться;
  - решение: проверить `pxf cluster status`, посмотреть логи PXF в `/data/pxf/logs`,
    перезапустить сервис `greenplum`.
- PXF «не подхватывает» изменения конфигов
  - причина: файлы уже лежат в `PXF_BASE` на томе, а seed из образа по умолчанию не перетирает их;
  - решение: `make build` + restart `greenplum` + (при необходимости) `PXF_SEED_OVERWRITE=1`.

## 9. Известная проблема: `protocol "pxf" does not exist` на «холодном старте» (исправлено)

Раньше (воспроизводилось в `./scripts/e2e_smoke.sh`) при первом `make ddl-gp` можно было получить:

`ERROR:  protocol "pxf" does not exist`

### Почему так происходило

В базовом `/start_gpdb.sh` из образа Greenplum создание расширения `pxf` связано с проверкой
файла `${PXF_BASE}/conf/pxf-env.sh`:

- если `pxf-env.sh` **отсутствует**, скрипт выполняет `pxf cluster prepare/register` и затем
  `CREATE EXTENSION IF NOT EXISTS pxf`;
- если `pxf-env.sh` **уже существует**, этот блок **пропускается**, и расширение может не появиться.

При этом наш ensure‑скрипт `pxf/init/10_pxf_bookings.sh` копировал `pxf-env.sh` в `${PXF_BASE}`
ещё до запуска Greenplum, из‑за чего базовый скрипт считал PXF “уже настроенным” и
пропускал создание расширения.

### Что изменили

- ensure‑скрипт больше не копирует `pxf-env.sh`, если файла ещё нет (даём `/start_gpdb.sh` создать его);
- в `start_greenplum_with_pxf.sh` добавлена retry‑логика с проверкой наличия extension;
- healthcheck `greenplum` ждёт не только PXF, но и наличие `extension pxf`.

### Если ошибка всё ещё возникает

1) Пересоберите образ и перезапустите контейнер `greenplum`:
`make build && make down && make up`

2) Проверьте наличие extension:
`docker compose exec greenplum bash -lc "su - gpadmin -c '/usr/local/greenplum-db/bin/psql -d gp_dwh -t -A -c \"SELECT extname FROM pg_extension WHERE extname = ''pxf'';\"'"`

## 8. Связанные файлы

- `Dockerfile.greenplum`
- `docker-compose.yml` (сервис `greenplum`: `build`, `hostname`, env, healthcheck)
- `pxf/init/10_pxf_bookings.sh` (ensure‑логика)
- `pxf/init/start_greenplum_with_pxf.sh` (старт контейнера)
- `README.md` (раздел «Greenplum + PXF: свой образ»)
