# Бенчмарк генерации bookings: параллельность и тюнинг PostgreSQL

> Дата: 2026-03-09
> Железо: Intel Core i7-11800H @ 2.30GHz (8 ядер / 16 потоков), игровой ноутбук
> Среда: WSL2, Docker Desktop, PostgreSQL 16 в контейнере
> Данные: seed-дамп 60 дней (~500k bookings, ~180k boarding_passes, ~1.2M tickets)

## Контекст

Генератор demodb (`postgrespro/demodb`) использует очередь событий `gen.events` с обработкой через `process_queue()`. При `jobs>1` воркеры запускаются через `dblink_send_query` и конкурируют за очередь через `SELECT ... FOR UPDATE SKIP LOCKED`.

Задача: найти оптимальную конфигурацию для `make bookings-generate-day` (инкремент +1 день).

## Методика

- Между экспериментами: `make bookings-init BOOKINGS_JOBS=N` (восстановление из seed-дампа, ~18 сек).
- Замер: `time make bookings-generate-day BOOKINGS_JOBS=N`.
- Контроль: `max(book_date)` должен сдвинуться на +1 день.
- Тюнинг PG: `ALTER SYSTEM SET` + `docker compose restart bookings-db`.

Параметры тюнинга PostgreSQL:
```
shared_buffers = 512MB      (дефолт: 128MB)
work_mem = 64MB             (дефолт: 4MB)
maintenance_work_mem = 256MB (дефолт: 64MB)
effective_cache_size = 1GB  (дефолт: 4GB)
wal_buffers = 16MB          (дефолт: -1, авто)
checkpoint_completion_target = 0.9 (дефолт: 0.9)
random_page_cost = 1.1      (дефолт: 4.0)
```

## Результаты

| # | Тюнинг PG | sync_commit | Jobs | Время  | vs baseline  |
|---|-----------|-------------|------|--------|--------------|
| 1 | нет       | on          | 1    | 2m 48s | **baseline** |
| 2 | нет       | on          | 2    | 8m 36s | 3× хуже      |
| 3 | да        | on          | 1    | 3m 01s | шум          |
| 4 | да        | on          | 2    | 8m 42s | 3× хуже      |
| 5 | да        | off         | 1    | 2m 50s | шум          |
| 6 | да        | off         | 2    | 8m 39s | 3× хуже      |

Повторный инкремент (прогретый кэш): 5m 03s (jobs=2), не замерялся (jobs=1).

## Выводы

### 1. jobs=1 оптимален для инкрементов

Для генерации +1 дня параллельность через dblink **контрпродуктивна**:
- Два воркера конкурируют за одну очередь `gen.events` через `FOR UPDATE SKIP LOCKED` — это row-level lock contention.
- Overhead: dblink-соединения, синхронизация через `gen.stat_jobs`, polling через `dblink_is_busy()`.
- На WSL2 дополнительно: виртуализированный I/O не масштабируется при параллельных записях.

При генерации с нуля (`make bookings-generate`, десятки тысяч событий) jobs>1 **может** давать прирост, но не тестировалось в этом бенчмарке.

### 2. Тюнинг PostgreSQL не влияет

Увеличение shared_buffers в 4 раза, work_mem в 16 раз и т.д. не дало измеримого эффекта. Узкое место — не буферы и не I/O, а сама логика генератора: последовательная обработка событий с COMMIT после каждого.

### 3. synchronous_commit=off не влияет

Генератор делает COMMIT после каждого события (~сотни раз за день). Ожидалось, что `synchronous_commit=off` ускорит запись WAL. Эффект не обнаружен — вероятно, WAL-буферы и так справляются при одном потоке.

### 4. VACUUM-ивенты — отдельная проблема

Генератор вставляет в очередь событие `VACUUM` каждую неделю модельного времени. Обработчик запускает `VACUUM ANALYZE` всей базы через `dblink_exec`. При 500k+ строках это занимает минуты. Решение: `DELETE FROM gen.events WHERE type = 'VACUUM'` перед `continue()` в инкрементальных скриптах.

## Итоговая конфигурация

```
BOOKINGS_JOBS=1         # синхронно, без dblink — ~3 мин на +1 день
BOOKINGS_INIT_DAYS=60   # seed-дамп покрывает ~34 дня бронирований + boarding_passes
```

Тюнинг PostgreSQL не требуется для текущих объёмов данных.
