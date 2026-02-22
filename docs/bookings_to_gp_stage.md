# DAG `bookings_to_gp_stage`: `bookings-db` → `stg` в Greenplum

Этот DAG — основной учебный пример в стенде. Он показывает путь данных из источника **Postgres**
(`bookings-db`, демо‑БД `demo`) в сырой слой **STG** в **Greenplum** с инкрементальной загрузкой
и простыми проверками качества данных.

## Что делает DAG

- В источнике (`bookings-db`) генерирует следующий учебный день данных в `bookings.bookings`
  (генератор всегда “шагает” вперёд от `max(book_date)`).
- В Greenplum загружает инкремент в `stg.bookings` через внешнюю таблицу `stg.bookings_ext`, используя PXF.
- Сверяет количество строк между источником (за окно инкремента) и загруженным батчем.
- Загружает инкремент в `stg.tickets` через внешнюю таблицу `stg.tickets_ext`, используя PXF.
- Запускает DQ‑проверки для `stg.tickets` (количество, ссылочная целостность, обязательные поля).
- Загружает справочники (full load): `stg.airports`, `stg.airplanes`, `stg.routes`, `stg.seats` + DQ.
- Загружает транзакции: `stg.flights` (инкремент), `stg.segments` (инкремент), `stg.boarding_passes` (full snapshot) + DQ.

## Что должно быть готово перед запуском

1) Стек поднят:

```bash
make up
```

2) Демо‑БД bookings установлена и содержит данные:

```bash
make bookings-init
```

3) В Greenplum созданы STG‑объекты (внешние `*_ext` через PXF и внутренние таблицы слоя `stg`)
для всех таблиц потока: `bookings`, `tickets`, `airports`, `airplanes`, `routes`, `seats`, `flights`,
`segments`, `boarding_passes` (выберите один вариант):

- учебный вариант: запустить DAG `bookings_stg_ddl` в Airflow UI;
- технический шорткат: `make ddl-gp`.

4) Airflow Connections:

- `bookings_db` — подключение к источнику `bookings-db`;
- `greenplum_conn` — подключение к Greenplum.

По умолчанию они задаются через переменные окружения `AIRFLOW_CONN_...` в `docker-compose.yml`,
поэтому могут не отображаться в UI — для `PostgresOperator` это нормально.

## Как запустить

1) Откройте Airflow UI: http://localhost:8080 (admin/admin).
2) Запустите DAG `bookings_to_gp_stage` вручную (Trigger DAG).
3) Дождитесь статуса Success у всех задач.

## Как это работает внутри (по шагам)

1) `generate_bookings_day`

- выполняет `sql/src/bookings_generate_day_if_missing.sql` в `bookings-db`;
- если `bookings.bookings` пустая — вызывает `generate(...)` на `BOOKINGS_INIT_DAYS`;
- иначе — вызывает `continue(...)`, добавляя ровно один следующий день.

2) `load_bookings_to_stg`

- выполняет `sql/stg/bookings_load.sql` в Greenplum;
- берёт строки из `stg.bookings_ext`, которые попадают в новое окно инкремента;
- вставляет их в `stg.bookings`, добавляя тех.колонки:
  - `src_created_at_ts` (опорная метка времени для инкремента),
  - `load_dttm`,
  - `batch_id={{ run_id }}`.

3) `check_row_counts`

- выполняет `sql/stg/bookings_dq.sql` в Greenplum;
- считает количество строк в источнике за то же окно инкремента и сравнивает с количеством строк,
  вставленных в `stg.bookings` для текущего `batch_id`;
- при расхождении делает `RAISE EXCEPTION` с понятным текстом.

4) `load_tickets_to_stg`

- выполняет `sql/stg/tickets_load.sql` в Greenplum;
- так как в `bookings.tickets` нет явной временной колонки, окно инкремента берётся по `book_date`
  из связанной внешней таблицы `stg.bookings_ext` (JOIN по `book_ref`);
- вставляет строки в `stg.tickets`, добавляя `src_created_at_ts`, `load_dttm` и `batch_id={{ run_id }}`.

5) `check_tickets_dq`

- выполняет `sql/stg/tickets_dq.sql` в Greenplum;
- проверяет количество строк в том же окне инкремента, а также ссылочную целостность и обязательные поля;
- при проблемах делает `RAISE EXCEPTION`, чтобы DAG падал “красным”.

6) Справочники (full load, параллельно где возможно)

Справочники загружаются “снэпшотом” (все строки) и затем проверяются DQ-скриптом.
Порядок определяется зависимостями данных — **airports** и **airplanes** грузятся **параллельно**,
потому что не зависят друг от друга:

```
check_tickets_dq
    ├─ load_airports  → check_airports_dq  ─┐
    │                                        ├─ load_routes → check_routes_dq
    └─ load_airplanes → check_airplanes_dq ─┤
                                             └─ load_seats  → check_seats_dq
```

- `load_airports_to_stg` → `check_airports_dq` (`sql/stg/airports_load.sql`, `sql/stg/airports_dq.sql`)
- `load_airplanes_to_stg` → `check_airplanes_dq` (`sql/stg/airplanes_load.sql`, `sql/stg/airplanes_dq.sql`)
- `load_routes_to_stg` → `check_routes_dq` (`sql/stg/routes_load.sql`, `sql/stg/routes_dq.sql`) — зависит от **airports** и **airplanes** (DQ проверяет ссылочную целостность)
- `load_seats_to_stg` → `check_seats_dq` (`sql/stg/seats_load.sql`, `sql/stg/seats_dq.sql`) — зависит от **airplanes** (DQ проверяет `airplane_code → airplanes`)

7) Транзакции

- `load_flights_to_stg` → `check_flights_dq` (инкремент по `scheduled_departure`) — зависит от **routes**
- `load_segments_to_stg` → `check_segments_dq` (инкремент по `book_date` через tickets/bookings) — зависит от **flights**
- `load_boarding_passes_to_stg` → `check_boarding_passes_dq` (full snapshot) — зависит от **segments**

Ветка `seats` работает параллельно с веткой `routes → flights → segments → boarding_passes`.
Обе ветки сходятся на `finish_summary`.

Важно: для инкрементальных таблиц “пустое окно инкремента” допустимо — загрузка и DQ логируют `NOTICE` и завершаются успешно.
Для snapshot-справочников (airports/airplanes/routes/seats) пустой источник считается ошибкой (DQ делает `RAISE EXCEPTION`).

8) `finish_summary`

- ждёт завершения **обеих** параллельных веток (`check_boarding_passes_dq` и `check_seats_dq`);
- логирует краткую сводку в конце запуска.

## Как проверить результат

```bash
make gp-psql
```

Примеры запросов:

```sql
SELECT COUNT(*) FROM stg.bookings;

SELECT
    src_created_at_ts,
    load_dttm,
    batch_id
FROM stg.bookings
ORDER BY src_created_at_ts DESC
LIMIT 10;
```

## Типичные ошибки

- `database "demo" does not exist`: демо‑БД не установлена → выполните `make bookings-init`.
- Ошибки про `stg.*`/`stg.*_ext`: не применён DDL → запустите `bookings_stg_ddl` или `make ddl-gp`.
- Ошибки PXF (`protocol "pxf" does not exist`, connection refused): перезапустите `greenplum` и повторите DDL.
  Для технических деталей см. `docs/internal/pxf_bookings.md`.

## Рекомендации по качеству решения

Ревью решения и список улучшений, которые делают пайплайн более “эталонным” для обучения:
`docs/internal/bookings_stg_code_review.md`.
