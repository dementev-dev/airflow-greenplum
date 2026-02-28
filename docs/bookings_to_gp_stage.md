# DAG `bookings_to_gp_stage`: `bookings-db` → `stg` в Greenplum

Этот DAG — основной учебный пример в стенде. Он показывает путь данных из источника **Postgres**
(`bookings-db`, демо‑БД `demo`) в сырой слой **STG** в **Greenplum** с инкрементальной загрузкой
и простой проверкой качества данных.

## Что делает DAG

- В источнике (`bookings-db`) генерирует следующий учебный день данных в `bookings.bookings`
  (генератор всегда “шагает” вперёд от `max(book_date)`).
- В Greenplum загружает инкремент в `stg.bookings` через внешнюю таблицу `stg.bookings_ext`, используя PXF.
- Сверяет количество строк между источником (за окно инкремента) и загруженным батчем.

## Что должно быть готово перед запуском

1) Стек поднят:

```bash
make up
```

2) Демо‑БД bookings установлена и содержит данные:

```bash
make bookings-init
```

Важно: генератор `bookings` в этом стенде поддерживается только в режиме `BOOKINGS_JOBS=1`.

3) В Greenplum созданы `stg.bookings_ext` и `stg.bookings` (выберите один вариант):

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

4) `finish_summary`

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
- Ошибки про `stg.bookings_ext`/`stg.bookings`: не применён DDL → запустите `bookings_stg_ddl` или `make ddl-gp`.
- Ошибки PXF (`protocol "pxf" does not exist`, connection refused): перезапустите `greenplum` и повторите DDL.
  Для технических деталей см. `docs/internal/pxf_bookings.md`.
