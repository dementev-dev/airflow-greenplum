# Временное ТЗ по блоку bookings (для текущей разработки)

_Внутренний файл для наставника: поясняет, как устроен источник `bookings-db` и генерация данных. Студентам обычно не нужен._

- Контейнер `bookings-db` — отдельный сервис Postgres из `docker-compose.yml`, база по умолчанию `demo` (из upstream demodb), без переименований.
- Доступ снаружи не блокируем (порт `5434` по умолчанию), чтобы позже читать через PXF и подключаться из Greenplum.
- Инициализация: два способа:
  - `make bookings-init` (рекомендуется): быстрое восстановление из seed-дампа (~18 сек).
  - `make bookings-generate` (для разработчиков): полная генерация с нуля — клонирует demodb с закреплённым коммитом, накладывает патчи (`engine`: `jobs=1` синхронно + `busy()` игнорирует свой pid; `install.sql`: `DROP DATABASE IF EXISTS`, `connstr` без хардкода), ждёт `pg_isready`, ставит `gen.connstr` и GUC `bookings.start_date/init_days/jobs`, затем запускает `/bookings/generate_next_day.sql` через `psql -f`. Значения по умолчанию: стартовая дата 2017-01-01, `init_days=60`, `jobs=2`.
- Генерация следующего дня: `make bookings-generate-day` прогоняет тот же SQL (читает GUC, вызывает `generate/continue`, ждёт `busy()`, закрывает dblink). При `jobs=1` всё синхронно, без dblink.
- Исходники demodb: клонируем по требованию с фиксированным хешем, кладём в `bookings/demodb/` (в `.gitignore`), патчи лежат в `bookings/patches/` и применяются автоматически при `make bookings-generate`.
- Документация: в README описаны команды (`bookings-init`, `bookings-generate`, проверка данных, генерация дня), параметры `.env`; настройка PXF/ETL — следующий этап.

## Текущее состояние
- `make bookings-init` — быстрое восстановление из seed-дампа (~18 сек), рекомендуется для студентов.
- `make bookings-generate` — полная генерация с нуля: автоматически применяет патчи (`engine_jobs1_sync.patch`, `install_drop_if_exists.patch`), ждёт готовности Postgres через `pg_isready`, запускает `install.sql`, выставляет `gen.connstr`/GUC и вызывает `generate_next_day.sql` через `psql -f`.
- Дефолты: `BOOKINGS_START_DATE=2017-01-01`, `BOOKINGS_INIT_DAYS=60`, `BOOKINGS_JOBS=2`. При `jobs=1` генерация идёт синхронно без dblink, `busy()` не учитывает текущую сессию.
- `.env.example`/README обновлены под новые дефолты; каталог `bookings/demodb/` в `.gitignore`.
- Патчи лежат в `bookings/patches/` и накладываются при `bookings-clone-demodb`.

## Текущее состояние тестов/проблем
- Чистый прогон `make bookings-init` (восстановление из seed-дампа) проходит за ~18 секунд.
- Чистый прогон `make bookings-generate` (после `docker compose down -v` и удаления `bookings/demodb`) проходит за ~1,5 минуты: база ставится, `busy()` → `f`, `bookings.bookings` от `2017-01-01 00:00:18` до `2017-01-01 23:59:59`.
- Ранее зависание на `busy()` при `jobs=1` лечится патчем: `process_queue` теперь синхронный, а `busy()` игнорирует текущий backend.
- Данных пока только на 1 день по умолчанию, чтобы генерация не занимала много времени.

## Идеи/следующие шаги
- Если понадобится больше дней — увеличивать `BOOKINGS_INIT_DAYS`, но помнить, что генерация может идти долго; контролировать через `SELECT busy();`.
- Следующий этап — PXF/ETL в Greenplum; текущая задача — лишь подготовить источник bookings.
