# План исправления: генератор demodb (bookings) остаётся пустым

## Контекст

В стенде используется демобаза bookings из репозитория `postgrespro/demodb`, закреплённая на коммите `d68de192850237719f09b47688d5f3fc94653ca6` (см. `DEMODB_COMMIT` в `Makefile`).

Инициализация источника для ETL выполняется командой `make bookings-init`:
- клонирует demodb в `bookings/demodb/`;
- пытается применить патчи из `bookings/patches/`;
- запускает `install.sql` в контейнере `bookings-db`;
- выставляет GUC-параметры (`gen.connstr`, `bookings.start_date/init_days/jobs`);
- запускает `/bookings/generate_next_day.sql` (должен сгенерировать минимум 1 день данных).

## Симптомы (как в TODO)

- после `make bookings-init` таблица `bookings.bookings` остаётся пустой;
- патчи `bookings/patches/engine_jobs1_sync.patch` и `bookings/patches/install_drop_if_exists.patch` падают при применении;
- из‑за этого DAG `bookings_to_gp_stage` валится на проверках (источник пустой).

## Предварительный диагноз (что уже видно)

1) `engine_jobs1_sync.patch` не является валидным unified diff (в hunk’ах нет номеров строк вида `@@ -N,M +N,M @@`), поэтому `patch` отвечает:
`patch: **** Only garbage was found in the patch input.`

2) `install_drop_if_exists.patch` устарел относительно закреплённого коммита demodb: в `install.sql` уже есть `DROP DATABASE IF EXISTS demo;`, поэтому hunk “не находится” и патч не накатывается.

3) Ошибки патча сейчас замаскированы в `Makefile` через `|| true`, поэтому `make bookings-init` может завершаться “успешно”, хотя критичные правки в demodb не применились.

## Цель фикса

- `make bookings-init` воспроизводимо создаёт и наполняет `demo.bookings.bookings` (>0 строк).
- Если патчи не применяются — процесс останавливается с понятным сообщением, что делать дальше.
- Патчи соответствуют закреплённому коммиту demodb и применяются идемпотентно.

## План диагностики (чтобы быстро подтвердить проблему)

1) Чистое воспроизведение:
- `make clean`
- `rm -rf bookings/demodb`
- `make bookings-init`

2) Проверка данных:
- `make bookings-psql`
- выполнить:
  - `SELECT COUNT(*) FROM bookings.bookings;`
  - `SELECT min(book_date), max(book_date) FROM bookings.bookings;`

3) Проверка патчей (без изменения файлов):
- `patch -d bookings/demodb -p1 --dry-run < bookings/patches/engine_jobs1_sync.patch`
- `patch -d bookings/demodb -p1 --dry-run < bookings/patches/install_drop_if_exists.patch`

Ожидаемо: сейчас dry-run показывает “garbage in patch” и/или “Hunk FAILED”.

## План решения

### Шаг 1. Пересобрать патчи под закреплённый демо‑коммит

Собираем патчи через `git diff`, чтобы получился корректный unified diff.

1) `bookings/patches/engine_jobs1_sync.patch`:
- Добавить/подтвердить 2 изменения в `engine.sql`:
  - `busy()` игнорирует текущий backend: `AND pid <> pg_backend_pid()`.
  - `continue()` при `jobs = 1` выполняет `process_queue(end_date)` синхронно и пишет заметный маркер в лог (`Job 1 (local): ok`), иначе — оставляет текущую логику через `dblink`.

2) `bookings/patches/install_drop_if_exists.patch`:
- Поменять строку (в актуальном `install.sql`):
  - было: `DROP DATABASE IF EXISTS demo;`
  - стало: `DROP DATABASE IF EXISTS demo WITH (FORCE);`

### Шаг 2. Сделать `make bookings-init` fail-fast на проблемах с патчами

В `Makefile`:
- убрать `|| true` у применения патчей;
- при ошибке патча — завершать `make` с ненулевым кодом и короткой подсказкой:
  - “удалите `bookings/demodb` и повторите `make bookings-init`”,
  - “если не помогло — проверьте, что `DEMODB_COMMIT` не менялся и патчи собраны под него”.

### Шаг 3. Добавить “защиту от тихого пустого результата”

После запуска `/bookings/generate_next_day.sql` (в `Makefile` или внутри SQL):
- выполнить проверку `COUNT(*)` по `bookings.bookings`;
- если 0 — завершаться ошибкой с подсказкой, куда смотреть (патчи/логи генератора).

Цель: чтобы проблема не уезжала дальше в DAG’и и DQ‑проверки, а ловилась сразу при init.

## Проверка (критерии готовности)

- `make clean && rm -rf bookings/demodb && make bookings-init` завершается без ошибок.
- `make bookings-psql` → `SELECT COUNT(*) FROM bookings.bookings;` возвращает `> 0`.
- `make bookings-generate-day` добавляет следующий день:
  - `max(book_date)` сдвигается на +1 сутки.
- `./scripts/e2e_smoke.sh` проходит до проверки `stg.bookings` (или хотя бы DAG `bookings_to_gp_stage` перестаёт падать на “источник пустой”).

## Откат (если нужно быстро вернуть стенд в рабочее состояние)

- Временно отключить применение патчей в `Makefile` и явно предупреждать, что генерация может быть нестабильной (нежелательно для студентов).
- Или зафиксировать альтернативный `DEMODB_COMMIT`, под который уже готовы патчи (делать только вместе с обновлением документации и проверкой, что генерация стабильна).

