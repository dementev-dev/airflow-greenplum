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

- создание `extension pxf` вынесено в `start_greenplum_with_pxf.sh` и обёрнуто ретраями;
- `pxf-env.sh` по‑прежнему копируется в `PXF_BASE`, чтобы `/start_gpdb.sh` не пытался выполнять
  `pxf cluster prepare` на непустом `PXF_BASE`;
- healthcheck `greenplum` ждёт не только PXF, но и наличие `extension pxf`.
- добавлен экспорт `PGPASSWORD` для `pxf cluster start`, чтобы `docker compose stop/start`
  не ломал запуск из‑за `password authentication failed` для `gpadmin`.

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

## 10. Известная проблема: после `docker compose stop/start` Greenplum может упасть (auth для PXF)

### Симптом

После `docker compose stop`, затем `docker compose start` контейнер `greenplum` иногда уходит в `Exited (1)`.
В логах видно, что GPDB поднялся, но упал на старте PXF:

- `INFO - pxf cluster start`
- `ERROR: Could not connect to GPDB`
- `FATAL: password authentication failed for user "gpadmin"`

### Текущее понимание причины (почему это “иногда”)

1) При старте GPDB образ `woblerr/greenplum` генерирует/дописывает `pg_hba.conf` на persistent volume.
2) В `pg_hba.conf` присутствует trust‑правило для **конкретного IP** контейнера в docker‑сети
   (пример из диагностики: `host all gpadmin 172.21.0.2/32 trust`).
3) После `docker compose stop/start` Docker может выдать контейнеру **другой IP** (например, `172.21.0.3`).
   Тогда trust‑правило больше не подходит, и подключение начинает идти по `md5`.
4) `pxf cluster start` подключается к GPDB по TCP на `host=gpdbsne` (hostname контейнера),
   то есть попадает именно в `pg_hba.conf` (а не в local‑auth).
5) В результате при “не совпавшем IP” получаем `md5` + пароль (возможно пустой/не тот) → падение на `28P01`.

Эта проблема выглядит флапающей, потому что IP после `stop/start` иногда совпадает с захардкоженным trust‑/32,
а иногда нет.

### Как подтвердить при следующем воспроизведении

1) Посмотреть логи `greenplum`:
`docker compose logs --tail=200 greenplum`

2) Найти реальный IP клиента в master‑логах GPDB (на томе):
`Password does not match ...` обычно содержит адрес вида `172.21.0.X`.

3) Сравнить его с trust‑строкой в `pg_hba.conf` на томе:
`/data/master/gpseg-1/pg_hba.conf`

Если IP в ошибке (например, `172.21.0.3`) **не** совпадает с trust‑/32 (например, `172.21.0.2/32`) —
это почти наверняка корень падения.

### Что с этим делать дальше (варианты решения, без реализации здесь)

Основная цель — убрать зависимость от “случайного IP после stop/start”:

- заставить `pxf cluster start` подключаться к GPDB через `127.0.0.1` (тогда работает существующий trust на localhost);
- или перестать добавлять в `pg_hba.conf` trust на конкретный `172.21.0.2/32` и заменить на более стабильное правило
  (например, на подсеть docker‑сети или на `samehost`);
- или закрепить IP контейнера в compose (static IP), чтобы он не “плавал”;
- или отказаться от `stop/start` в пользу сценария, который не меняет сетевое окружение (но это хуже для UX студентов).

### Что реализовано

- В `pxf/init/start_greenplum_with_pxf.sh` добавлен шаг, который на каждом старте
  обеспечивает в `pg_hba.conf` trust‑правило `host all gpadmin samehost trust`
  (вставка перед `host all all 0.0.0.0/0 md5`), и делает `pg_ctl reload`, если GPDB уже запущен.
