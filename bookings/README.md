# Демобаза bookings в Postgres

Этот каталог используется для работы с демобазой [bookings](https://postgrespro.ru/education/demodb), которая будет источником данных для будущего DWH в Greenplum.

На первом этапе мы:
- поднимаем отдельный контейнер `bookings-db` с Postgres;
- инициализируем демобазу одним из двух способов (см. ниже);
- генерируем данные «день за днём» с помощью `make`‑команд.

### Два способа инициализации

| Команда | Что делает | Время | Для кого |
|---------|-----------|-------|----------|
| `make bookings-init` | Быстрое восстановление из seed-дампа | ~18 сек | **Студенты** (рекомендуется по умолчанию) |
| `make bookings-generate` | Полная генерация с нуля через генератор demodb | часы | Разработчики, пересоздание дампа |

Основные команды см. в корневом `Makefile` (`bookings-init`, `bookings-generate`, `bookings-generate-day`, `bookings-psql`) и в `README.md` проекта.

## Источник и версия
- Репозиторий демобазы: `postgrespro/demodb`.
- Закреплённый коммит: `866e56f7` (см. `DEMODB_COMMIT` в корневом `Makefile`).

## Что мы патчим в demodb
- `install.sql`: `DROP DATABASE IF EXISTS demo WITH (FORCE)` — установка не падает, даже если демобазу держат активные сессии (например, из Airflow).
- `engine.sql`: два изменения в `engine_jobs1_sync.patch`:
  - `busy()` игнорирует свой `pid`, чтобы не считать собственное подключение занятым;
  - `continue()` при `jobs=1` вызывает `process_queue` синхронно (без `dblink`), иначе генерация обрывается при выходе из `psql` и данных не появляется.
- `install.sql`: удалён хардкод `gen.connstr` без credentials (`install_connstr_no_hardcode.patch`).
- Дефолт: `BOOKINGS_JOBS=1` (синхронно, без dblink — оптимально для +1 дня, ~3 мин). При `jobs>1` — через dblink, но на WSL2 в 3× медленнее из-за lock contention на `gen.events`.
- Патчи применяются автоматически в `make bookings-generate` (генерация с нуля). Если что-то пошло не так, их можно накатить вручную:
  ```
  patch -d bookings/demodb -p1 --forward < bookings/patches/install_drop_if_exists.patch
  patch -d bookings/demodb -p1 --forward < bookings/patches/engine_jobs1_sync.patch
  ```

## Быстрая проверка после init/обновления
- `make bookings-init` (восстановление из дампа) должен завершиться без ошибок; в `bookings.bookings` ожидаем >0 строк (примерно 15k).
- `make bookings-generate` (генерация с нуля) тоже должен дать >0 строк, но занимает значительно больше времени.
- `make bookings-generate-day` добавляет следующий день после `max(book_date)`.
- Ручной вызов генерации из psql/DBeaver — только через DO-блок (подзапрос в аргументах `CALL` не работает):
  ```sql
  DO $$
  DECLARE
    v_next_day timestamptz;
  BEGIN
    SELECT date_trunc('day', max(book_date)) + interval '1 day'
    INTO v_next_day
    FROM bookings.bookings;
    CALL continue(v_next_day, 1);
  END $$;
  ```
