# План работ: свой образ Greenplum с интегрированным PXF

## Контекст и проблема

Иногда (и у нас воспроизводится стабильно) контейнер `greenplum` падает при старте с ошибкой:

`chown: changing ownership of '/docker-entrypoint-initdb.d/10_pxf_bookings.sh': Read-only file system`

Причина: init-скрипт `pxf/init/10_pxf_bookings.sh` проброшен в контейнер как bind-mount `:ro`, а entrypoint базового образа пытается сделать `chown` файлов в `/docker-entrypoint-initdb.d/`.

## Цели

- Greenplum стабильно стартует после любого `restart/up` (без “иногда не стартует”).
- PXF готов к работе **после каждого запуска** контейнера.
- Не меняем права/владельца файлов в репозитории на хосте (никаких “файл стал root’ом / не редактируется”).
- Для студентов всё остаётся простым: `make build` (явная сборка) + `make up` (поднимает стенд; если образа нет — Docker Compose соберёт сам).

## Выбранный подход (высокоуровневый дизайн)

1) Делаем свой образ Greenplum на базе `woblerr/greenplum:6.27.1`.

2) Встраиваем в образ “seed” для PXF:
   - JDBC-драйвер (JAR),
   - конфиг сервера `bookings-db` (`jdbc-site.xml`),
   - скрипт “ensure”, который идемпотентно гарантирует, что файлы лежат в `PXF_BASE` (на persistent volume).

3) Запускаем “ensure” **на каждом старте контейнера** через wrapper-entrypoint, а затем передаём управление оригинальному entrypoint базового образа.

4) `PXF_BASE` по умолчанию остаётся на volume (`/data/pxf`), чтобы настройки переживали рестарты.

## План работ (по шагам)

### Шаг 1. Разведка базового образа

- Проверить, где находится оригинальный entrypoint и как он запускается (путь, параметры, пользователь).
- Понять, как `GREENPLUM_PXF_ENABLE=true` влияет на старт (чтобы wrapper не ломал поведение).

Результат: фиксируем в README “как устроен старт” (1–2 абзаца).

### Шаг 2. Новый Dockerfile для Greenplum

- Добавить `Dockerfile.greenplum`:
  - `FROM woblerr/greenplum:6.27.1`
  - `COPY` seed-артефакты в образ (например, в `/opt/pxf-seed/...`)
  - `COPY` wrapper-entrypoint в образ
  - настроить права/владельца внутри образа так, чтобы старт был без ошибок

Результат: образ собирается локально через `make build` и/или автоматически через `make up`.

### Шаг 3. Wrapper-entrypoint (каждый старт)

- Добавить скрипт entrypoint-обёртки (например, `greenplum/entrypoint-wrapper.sh` или `pxf/entrypoint-wrapper.sh`):
  - на старте вызывает `ensure`-скрипт;
  - затем делает `exec` оригинального entrypoint базового образа с теми же аргументами.

Важно: wrapper не должен “перехватывать” логику инициализации кластера — только добавлять шаг подготовки PXF.

### Шаг 4. Переписать текущий init-скрипт в “ensure” (идемпотентный)

- Превратить `pxf/init/10_pxf_bookings.sh` в скрипт, который можно безопасно выполнять на каждом запуске:
  - не опираться на `~/.bashrc`;
  - `PXF_BASE` вычислять через env (`PXF_BASE`, `GREENPLUM_DATA_DIRECTORY`, fallback `/data/pxf`);
  - seed-копирование делать “если файла нет”;
  - добавить понятные логи (что сделано / что пропущено);
  - `pxf cluster sync`:
    - выполнять, только если команда доступна,
    - не валить контейнер при ошибке (но писать предупреждение).

Дополнительно (опционально, но полезно для стенда):
- env-переключатель `PXF_SEED_OVERWRITE=1` — принудительно перезаписывать конфиг из образа в volume (для обновлений без удаления volume).

### Шаг 5. Обновить `docker-compose.yml`

- Для сервиса `greenplum` перейти на `build:` (и при желании оставить `image:` как тег).
- Убрать bind-mount’ы PXF (jar/config/init-скрипт), т.к. теперь всё в образе.
- Оставить `greenplum_data:/data` и `./sql:/sql:ro`.
- Исправить healthcheck Greenplum (сейчас конструкция вида `... || echo 1` делает healthcheck “вечно успешным”):
  - healthcheck должен возвращать ненулевой код, если БД не готова;
  - добавить `start_period`, чтобы не ловить ложные падения на холодном старте.

### Шаг 6. Обновить Makefile и README

- `Makefile`:
  - убедиться, что `make build` собирает также Greenplum-образ (если введём build для сервиса);
  - оставить `make up` как есть (Compose сам соберёт образ, если его нет).
- `README.md`:
  - зачем свой образ (устойчивость, права на хосте, меньше mount’ов),
  - как пересобрать образ,
  - как обновить PXF-конфиг (через rebuild + `PXF_SEED_OVERWRITE=1` или через очистку volume).

## Проверка и критерии готовности

- `docker compose up -d greenplum` → контейнер остаётся `Up`, не падает.
- `docker compose restart greenplum` повторить 10–20 раз → без падений.
- Healthcheck Greenplum становится `healthy` (не “вечно healthy” и не “вечно starting”).
- Поднятие всего стека (`make up`) приводит к старту Airflow (scheduler/webserver), т.к. `depends_on: condition: service_healthy` начинает работать корректно.

## Риски и как их снизить

- **Стартап может замедлиться**, если `pxf cluster sync` делать каждый раз: поэтому скрипт должен быть быстрым, а sync — не фатальным при ошибках.
- **Обновление конфигов**: так как PXF_BASE на volume, изменения в образе сами не перетрут файлы — поэтому нужен `PXF_SEED_OVERWRITE=1` или понятная инструкция “как обновить”.

## Откат

- Вернуться к использованию `image: woblerr/greenplum:6.27.1` в `docker-compose.yml`.
- Вернуть mount’ы PXF, если нужно (но это вернёт риск с `:ro`).

---

## Статус (реализовано)

- Добавлен кастомный образ Greenplum: `Dockerfile.greenplum` (seed PXF + startup wrapper).
- Ensure‑скрипт перенесён в образ и стал идемпотентным: `pxf/init/10_pxf_bookings.sh`.
- Стартовый скрипт контейнера включает ensure и создаёт `EXTENSION pxf`: `pxf/init/start_greenplum_with_pxf.sh`.
- В `docker-compose.yml`:
  - `greenplum` собирается через `build: Dockerfile.greenplum`;
  - добавлен `hostname: gpdbsne`;
  - убраны PXF bind-mount’ы (jar/config/init);
  - healthcheck ждёт не только GPDB, но и готовность PXF.
- Обновлены инструкции: `README.md`, `.env.example`.
- Проблема с генератором demodb (пустая `bookings.bookings`) зафиксирована в `TODO.md`.

## Проверка (как воспроизвести)

Команды для ручной проверки:

- Пересобрать и перезапустить Greenplum:
  - `make build`
  - `docker compose up -d --force-recreate greenplum`
- 3–10 рестартов:
  - `docker compose restart greenplum`
  - дождаться `healthy` в `docker compose ps`
- Проверить PXF:
  - `docker compose exec greenplum bash -lc "su - gpadmin -c '/usr/local/pxf/bin/pxf cluster status'"`
- Проверить DAG, который использует PXF:
  - `docker exec -i gp_airflow_scheduler airflow dags test bookings_stg_ddl`

Текущий результат:

- `bookings_stg_ddl` проходит (PXF и `protocol pxf` доступны).
- `bookings_to_gp_stage` падает не из-за PXF, а из-за пустого источника
  (`demo.bookings.bookings` = 0 строк). Это отдельная задача (см. `TODO.md`).
