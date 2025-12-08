# Временное ТЗ по блоку bookings (для текущей разработки)

_Этот файл внутренний, удалить перед итоговой сдачей._

- Контейнер `bookings-db` — отдельный сервис Postgres из `docker-compose.yml`, база по умолчанию `demo` (из upstream demodb), без переименований.
- Доступ снаружи не блокируем (порт `5434` по умолчанию), чтобы позже читать через PXF и подключаться из Greenplum.
- Инициализация (`make bookings-init`): поднимает контейнер, клонирует demodb (без автопатчей), запускает `install.sql` как есть; генерация стартовых данных должна проходить штатными `generate/continue`.
- Генерация следующего дня: либо `make bookings-generate-day` вызывает `CALL continue(...)`, либо ручной вызов; скрипт `generate_next_day.sql` опционален и не должен менять upstream SQL (только тонкая обертка при желании).
- Исходники demodb: клонировать по требованию с закрепленным коммитом (чтобы не ловить неожиданные изменения). Добавить в `.gitignore`, если не коммитим. Завендорить в репо можно позже, если лицензия позволит.
- Документация: в README описать шаги (`bookings-init`, проверка данных, генерация дня), параметры `.env`, и что настройка PXF/ETL — следующий этап.

## Текущее состояние
- `Makefile` очищен: убраны автопатчи demodb; добавлены `DEMODB_REPO/DEMODB_COMMIT` с фиксированным хешем `d68de1…`, таргет снова исполняется. `BOOKINGS_JOBS` задаёт число джобов генерации (по умолчанию 1).
- `.gitignore` дополнен `bookings/demodb/` (клонируем по требованию, не коммитим).
- `bookings-init` теперь после установки запускает генерацию (дата из `BOOKINGS_START_DATE`, дни из `BOOKINGS_INIT_DAYS`, параллельность `BOOKINGS_JOBS`), используя обертку `generate_next_day.sql`.
- `generate_next_day.sql` читает GUC `bookings.start_date/init_days/jobs`, вызывает `generate/continue`, ждёт `busy()`, закрывает dblink.
- Генерация через `make` остаётся нестабильной: в логах обрывы dblink, данные не появляются.

## Текущее состояние тестов/проблем
- Прогон `make bookings-init` с `BOOKINGS_JOBS=2`, `BOOKINGS_INIT_DAYS=30`: установка ок, GUC/`gen.connstr` выставлены, `Starting job 1/2: ok`, но `bookings.bookings` пустая, в `gen.events` висит `INIT`, `busy()` может быть `t`. Логи: `could not send data to client: Broken pipe` / `connection to client lost` при `TRUNCATE ... CASCADE` в `do_init`.
- С `BOOKINGS_JOBS=1` аналогично. Ручная генерация в DBeaver (CALL generate(now(), now()+interval '1 year')) ранее шла медленно, так что проблема может быть в запуске через `docker exec`/pipe или в dblink-подключениях.

## Идеи/следующие шаги
- Проверить генерацию внутри контейнера через here-doc без пайпа:
  ```
  docker compose exec -T bookings-db psql -U bookings -d demo <<'SQL'
  SET bookings.start_date = '2017-01-01';
  SET bookings.init_days = '30';
  SET bookings.jobs = '1';
  \i /bookings/generate_next_day.sql
  SQL
  ```
  Наблюдать `busy()`, `gen.log`, `gen.events/events_history`.
- Если так ок — переписать Makefile под такой запуск. Если нет — попробовать `gen.connstr` с `host=localhost` и/или оставить `jobs=1` без dblink.
- Пока нестабильно, держать `init_days` небольшим (1–3), чтобы быстро видеть прогресс.
