# Демобаза bookings в Postgres

Этот каталог используется для работы с демобазой [bookings](https://postgrespro.ru/education/demodb), которая будет источником данных для будущего DWH в Greenplum.

На первом этапе мы:
- поднимаем отдельный контейнер `bookings-db` с Postgres;
- устанавливаем в нём генератор демобазы `demodb` (репозиторий `postgrespro/demodb`);
- генерируем данные «день за днём» с помощью `make`‑команд.

Основные команды см. в корневом `Makefile` (`bookings-init`, `bookings-generate-day`, `bookings-psql`) и в `README.md` проекта.

## Источник и версия
- Репозиторий демобазы: `postgrespro/demodb`.
- Закреплённый коммит: `d68de192850237719f09b47688d5f3fc94653ca6` (см. `DEMODB_COMMIT` в корневом `Makefile`).

## Что мы патчим в demodb
- `install.sql`: `DROP DATABASE IF EXISTS demo WITH (FORCE)` — установка не падает, даже если демобазу держат активные сессии (например, из Airflow).
- `engine.sql`: два изменения в `engine_jobs1_sync.patch`:
  - `busy()` игнорирует свой `pid`, чтобы не считать собственное подключение занятым;
  - `continue()` при `jobs=1` вызывает `process_queue` синхронно (без `dblink`), иначе генерация обрывается при выходе из `psql` и данных не появляется.
- Режим эксплуатации в этом стенде: только `jobs=1` (`BOOKINGS_JOBS=1`).
- Патчи применяются автоматически в `make bookings-init`. Если что-то пошло не так, их можно накатить вручную:
  ```
  patch -d bookings/demodb -p1 --forward < bookings/patches/install_drop_if_exists.patch
  patch -d bookings/demodb -p1 --forward < bookings/patches/engine_jobs1_sync.patch
  ```

## Быстрая проверка после init/обновления
- `make bookings-init` должен завершиться без ошибок; в `bookings.bookings` ожидаем >0 строк (примерно 15k).
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
