# DAG `bookings_to_gp_ods`: `stg` → `ods` в Greenplum

Этот DAG — учебный пример загрузки типизированного слоя **ODS** из уже подготовленного слоя **STG**.
Логика каноничная: **TRUNCATE + INSERT** для snapshot-справочников, **SCD1 UPSERT** для транзакционных
таблиц + DQ-проверки.

## Что делает DAG

- Определяет `stg_batch_id` — последний согласованный батч, по которому все 4 snapshot-справочника
  (`airports`, `airplanes`, `routes`, `seats`) уже приехали в STG.
- Загружает 9 таблиц ODS: `bookings`, `tickets`, `airports`, `airplanes`, `routes`, `seats`,
  `flights`, `segments`, `boarding_passes` (на ветке `main` загрузка `airplanes` и `seats` —
  заглушки; реализуйте их по ТЗ в `docs/assignment/analyst_spec.md`).
- Для каждой таблицы выполняет пару задач `load → dq`.
- Snapshot-справочники фильтруются по `stg_batch_id`, транзакционные таблицы — по HWM (`_load_ts`).
- Для snapshot-справочников дополнительно синхронизирует ключи (удаляет из ODS записи,
  отсутствующие в выбранном STG-батче).
- Для `flights` дополнительно добирает рейсы из истории `stg.flights`, если на них
  ссылаются `stg.segments` (чтобы сохранить ссылочную целостность `segments.flight_id → flights.flight_id`).

## Что должно быть готово перед запуском

1) Стек поднят:

```bash
make up
```

2) STG-слой создан и заполнен:

- запущен `bookings_stg_ddl` (или `make ddl-gp`);
- хотя бы один раз выполнен DAG `bookings_to_gp_stage`.

3) ODS-таблицы созданы (один из вариантов):

- учебный: запустить DAG `bookings_ods_ddl`;
- шорткат: `make ddl-gp` (создаёт и STG, и ODS).

## Как запустить

1) Откройте Airflow UI: http://localhost:8080.
2) Запустите DAG `bookings_to_gp_ods`.
3) (Опционально) передайте `stg_batch_id` в конфиге запуска:

```json
{"stg_batch_id": "manual__2026-02-22T12:00:00+00:00"}
```

Если конфиг не передан, DAG автоматически возьмёт последний согласованный snapshot-батч.

## Граф зависимостей

```
resolve_stg_batch_id
    ├─ load_ods_bookings → dq_ods_bookings
    │      └─ load_ods_tickets → dq_ods_tickets ──────────────────┐
    │                                                              │
    ├─ load_ods_airports  → dq_ods_airports  ─┐                   │
    │                                          ├─ load_ods_routes  │
    ├─ load_ods_airplanes → dq_ods_airplanes ─┤     └─ dq_ods_routes
    │                                          │           └─ load_ods_flights
    │                                          │                └─ dq_ods_flights ─┐
    │                                          │                                   │
    │                                          │     dq_ods_flights + dq_ods_tickets
    │                                          │           └─ load_ods_segments
    │                                          │                └─ dq_ods_segments
    │                                          │                     └─ load_ods_boarding_passes
    │                                          │                          └─ dq_ods_boarding_passes ─┐
    │                                          │                                                     │
    │                                          └─ load_ods_seats                                     │
    │                                               └─ dq_ods_seats ─────────────────────────────────┤
    │                                                                                                │
    └────────────────────────────────────────────────────────────────── finish_ods_summary ◀──────────┘
```

Ветка `seats` работает параллельно с веткой `routes → flights → segments → boarding_passes`.
Обе ветки сходятся на `finish_ods_summary`.

## Как это работает внутри (по шагам)

### 1) `resolve_stg_batch_id` (Python)

Определяет, какой STG-батч использовать для snapshot-справочников.
Если `stg_batch_id` не передан через `dag_run.conf`, ищет последний **согласованный** батч —
`_load_id`, который есть одновременно во всех четырёх snapshot-таблицах
(`stg.airports`, `stg.airplanes`, `stg.routes`, `stg.seats`).
Для этого используется **INTERSECT** по `_load_id`.

> **Зачем согласованность?** Чтобы ODS загружал только те данные, для которых приехали
> ВСЕ связанные справочники. Иначе возможна потеря ссылочной целостности при сборке витрин.

### 2–10) Загрузка 9 таблиц: `load_ods_*` → `dq_ods_*`

Каждая пара задач работает одинаково:

| # | Задача | SQL-файл | Тип загрузки |
|---|--------|----------|--------------|
| 2 | `load_ods_bookings` → `dq_ods_bookings` | `sql/ods/bookings_load.sql`, `sql/ods/bookings_dq.sql` | HWM (инкремент) |
| 3 | `load_ods_tickets` → `dq_ods_tickets` | `sql/ods/tickets_load.sql`, `sql/ods/tickets_dq.sql` | HWM (инкремент) |
| 4 | `load_ods_airports` → `dq_ods_airports` | `sql/ods/airports_load.sql`, `sql/ods/airports_dq.sql` | snapshot по `stg_batch_id` |
| 5 | `load_ods_airplanes` → `dq_ods_airplanes` | `sql/ods/airplanes_load.sql`, `sql/ods/airplanes_dq.sql` | snapshot по `stg_batch_id` ⚠️ заглушка на main |
| 6 | `load_ods_routes` → `dq_ods_routes` | `sql/ods/routes_load.sql`, `sql/ods/routes_dq.sql` | snapshot по `stg_batch_id` |
| 7 | `load_ods_seats` → `dq_ods_seats` | `sql/ods/seats_load.sql`, `sql/ods/seats_dq.sql` | snapshot по `stg_batch_id` ⚠️ заглушка на main |
| 8 | `load_ods_flights` → `dq_ods_flights` | `sql/ods/flights_load.sql`, `sql/ods/flights_dq.sql` | HWM (инкремент) |
| 9 | `load_ods_segments` → `dq_ods_segments` | `sql/ods/segments_load.sql`, `sql/ods/segments_dq.sql` | HWM (инкремент) |
| 10 | `load_ods_boarding_passes` → `dq_ods_boarding_passes` | `sql/ods/boarding_passes_load.sql`, `sql/ods/boarding_passes_dq.sql` | HWM (инкремент) |

### Два паттерна загрузки

В ODS используются **два разных паттерна** — выбор зависит от типа данных и формата хранения:

**Snapshot-справочники** (`airports`, `airplanes`, `routes`, `seats`) — **TRUNCATE + INSERT**:

1. **TRUNCATE** — полная очистка таблицы.
2. **INSERT** — вставка всех строк из STG-батча (`_load_id = stg_batch_id`) с дедупликацией
   через `ROW_NUMBER()`.

> Почему не UPSERT? Эти таблицы хранятся в формате **AO Row** (`appendonly=true`),
> который не поддерживает эффективный row-level UPDATE (вызывает bloat).
> Для маленьких справочников (~100–300 строк) полная перезагрузка быстрее и чище.

> На ветке `main` загрузка `airplanes` и `seats` — заглушки.
> Паттерн TRUNCATE + INSERT описан выше; используйте `airports_load.sql` как образец.

**Транзакционные таблицы** (`bookings`, `tickets`, `flights`, `segments`, `boarding_passes`) —
**SCD1 UPSERT**:

1. **TEMP TABLE** — собирает дельту (новые/изменённые строки) с дедупликацией внутри батча
   через `ROW_NUMBER()`. Временная таблица автоматически удаляется (`ON COMMIT DROP`).
2. **UPDATE** — обновляет существующие строки. Использует `IS DISTINCT FROM` для корректного
   сравнения `NULL`-значений (обычный `<>` не обнаружит изменение `NULL → значение`).
3. **INSERT** — добавляет новые строки (которых нет в ODS по бизнес-ключу).

Транзакционные таблицы фильтруются по **HWM** — `WHERE _load_ts > (SELECT MAX(_load_ts) FROM ods.table)`.
Это сделано, чтобы не потерять инкременты, если STG-DAG запускался несколько раз
до запуска ODS-DAG'а.

### DQ-проверки (одинаковый паттерн)

Каждый `*_dq.sql` — PL/pgSQL-блок (`DO $$...$$`), который проверяет:
- нет дублей по бизнес-ключу в ODS;
- все ключи из STG текущего батча присутствуют в ODS;
- обязательные поля не содержат NULL.

При ошибке — `RAISE EXCEPTION` с понятным текстом. Для инкрементальных таблиц пустой батч допустим.

### 11) `finish_ods_summary`

Ждёт завершения обеих параллельных веток (`dq_ods_boarding_passes` и `dq_ods_seats`)
и логирует краткую сводку.

## Как проверить результат

```bash
make gp-psql
```

```sql
SELECT COUNT(*) FROM ods.bookings;
SELECT COUNT(*) FROM ods.tickets;
SELECT COUNT(*) FROM ods.flights;

-- Проверка: в ODS не должно быть дублей по бизнес-ключу
SELECT book_ref, COUNT(*)
FROM ods.bookings
GROUP BY 1
HAVING COUNT(*) > 1;
```

Ожидаемо: в последнем запросе `0` строк.

## Типичные ошибки

- `stg_batch_id не найден`:
  - передайте `stg_batch_id` в `dag_run.conf`, или
  - сначала загрузите STG через `bookings_to_gp_stage`.
- Ошибки `relation "ods...." does not exist`:
  - не применён ODS DDL (`bookings_ods_ddl` / `make ddl-gp`).
- Ошибки DQ по ссылочной целостности:
  - проверьте, что ODS DAG выполнялся с корректным `stg_batch_id` и без пропуска upstream задач.
