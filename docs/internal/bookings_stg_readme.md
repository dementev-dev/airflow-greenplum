# Мини‑README по учебному DAG bookings_to_gp_stage (черновик)

_Внутренний файл, чтобы не забыть договорённости. Перед итоговой сдачей документацию по блоку bookings/STG нужно будет аккуратно собрать и переписать._

## 1. Что делает DAG

- DAG `bookings_to_gp_stage` показывает учебный поток:
  - источник: демо‑БД `bookings-db` (Postgres, схема `bookings`, таблица `bookings.bookings`);
  - при каждом запуске генерируется один учебный день данных (идемпотентно);
  - данные из `bookings.bookings` переливаются в сырой слой `stg.bookings` в Greenplum через PXF‑внешнюю таблицу `stg.bookings_ext`.
- Слой `stg` задуман как «сырой»:
  - все бизнес‑колонки (`book_ref`, `book_date`, `total_amount`) хранятся как `TEXT`;
  - есть тех.колонки `src_created_at_ts`, `load_dttm`, `batch_id`.

Подробный дизайн описан в `docs/internal/bookings_stg_design.md`.

## 2. Что нужно, чтобы DAG завёлся

Минимальные предпосылки:

- Стенд поднят: `make up`.
- Демо‑БД bookings инициализирована: `make bookings-init`.
- В Greenplum применён DDL (созданы схема `stg` и таблицы `stg.bookings_ext` / `stg.bookings`):
  - учебный вариант: запустить DAG `bookings_stg_ddl` (он использует `sql/stg/bookings_ddl.sql`);
  - технический шорткат: `make ddl-gp` применяет все DDL разом вручную. Команда сама не вызывается при старте контейнеров, её нужно запустить явно.
- В Airflow есть коннекты:
  - `greenplum_conn` (по умолчанию уже используется в helpers/greenplum.py);
  - `bookings_db` (Postgres к сервису `bookings-db`, если не хочется полагаться на ENV).

## 3. Последовательность задач в DAG

- `generate_bookings_day`:
  - PostgresOperator к `bookings-db`;
  - выполняет SQL `/sql/src/bookings_generate_day_if_missing.sql`;
  - скрипт смотрит на `max(book_date)` в `bookings.bookings`:
    - если база пустая — берёт стартовую дату из GUC и генерирует `bookings.init_days` суток;
    - если данные уже есть — добавляет один следующий учебный день после `max(book_date)` и пишет NOTICE с интервалом генерации.
- `load_bookings_to_stg`:
  - PostgresOperator к Greenplum;
  - выполняет SQL `/sql/stg/bookings_load.sql`;
  - считает «старый» максимум `src_created_at_ts` (по предыдущим батчам) и грузит только новые строки из `stg.bookings_ext`, заполняя `src_created_at_ts`, `load_dttm`, `batch_id={{ ds_nodash }}`.
- `check_row_counts`:
  - PostgresOperator к Greenplum;
  - выполняет SQL `/sql/stg/bookings_dq.sql`;
  - за то же окно инкремента считает количество строк в источнике и в `stg.bookings` (по текущему `batch_id`);
  - при расхождении делает `RAISE EXCEPTION` с понятным текстом ошибки.
- `finish_summary`:
  - логирует итог выполнения DAG за одно срабатывание.

## 4. Как этим пользоваться студенту (черновой сценарий)

1. Поднять стенд и подготовить источники:
   - `make up`
   - `make airflow-init`
   - `make bookings-init`
   - `make ddl-gp`
2. Открыть Airflow UI (`http://localhost:8080`) и включить DAG `bookings_to_gp_stage`.
3. Вызвать `Trigger` DAG (дату логического запуска можно оставить по умолчанию — она используется только как метка `batch_id`).
4. Посмотреть:
   - в `bookings-db` ― что появился день с бронированиями;
   - в Greenplum (`make gp-psql`) — данные в `stg.bookings`:
     - `SELECT * FROM stg.bookings LIMIT 10;`
     - `SELECT src_created_at_ts, load_dttm, batch_id FROM stg.bookings ORDER BY src_created_at_ts DESC LIMIT 10;`
5. Перезапустить DAG ещё несколько раз и увидеть, что:
   - генерация в `bookings.bookings` идёт по одному дню вперёд от текущего `max(book_date)`;
   - в `stg.bookings` появляются только новые записи (delta), помеченные разными `batch_id`.

## 5. Примечания «на потом»

- Текущая документация по блоку bookings/STG разбросана:
  - `README.md` (общий обзор стенда),
  - `docs/internal/bookings_tz.md` (источник bookings),
  - `docs/internal/pxf_bookings.md` (PXF),
  - `docs/internal/bookings_stg_design.md` (дизайн STG),
  - этот файл (мини‑README по DAG).
- В будущем всё это нужно будет собрать в одну понятную историю для студента:
  - отдельный раздел «Учебный пример: bookings → stg → dwh»;
  - скриншоты DAG, примеры запросов и типичные ошибки.
