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
  - `make ddl-gp` (использует `sql/ddl_gp.sql`).
- В Airflow есть коннекты:
  - `greenplum_conn` (по умолчанию уже используется в helpers/greenplum.py);
  - `bookings_db` (Postgres к сервису `bookings-db`, если не хочется полагаться на ENV).

## 3. Последовательность задач в DAG

- `generate_bookings_day`:
  - проверяет наличие таблицы `bookings.bookings`;
  - смотрит, есть ли строки за `load_date` (`book_date::date = load_date`);
  - если день уже сгенерирован — ничего не делает (идемпотентность), просто логирует это;
  - если нет — запускает генератор через DO‑блок (логика как в `bookings/generate_next_day.sql`).
- `get_last_loaded_ts_from_gp`:
  - проверяет, что есть таблица `stg.bookings`;
  - берёт `max(src_created_at_ts)` как последнюю загруженную метку;
  - если NULL — значит в STG ещё нет данных, дальше идём в режим `full`.
- `extract_and_load_increment_via_pxf`:
  - читает данные из `stg.bookings_ext` и вставляет в `stg.bookings`;
  - при `last_loaded_ts is None` делает полную загрузку (full) — все строки;
  - при delta берёт только строки с `book_date > last_loaded_ts` и `book_date <= load_date + 1 day`;
  - приводит бизнес‑поля к `TEXT`, заполняет `src_created_at_ts`, `load_dttm`, `batch_id={{ ds_nodash }}`.
- `check_row_counts`:
  - пересчитывает количество строк в источнике (`stg.bookings_ext`) за текущее окно (full/delta);
  - сравнивает с количеством строк в `stg.bookings` для текущего `batch_id`;
  - если не сходится — падает с понятным сообщением и подсказкой «куда смотреть».
- `finish_summary`:
  - логирует итог выполнения DAG за одно срабатывание.

## 4. Как этим пользоваться студенту (черновой сценарий)

1. Поднять стенд и подготовить источники:
   - `make up`
   - `make airflow-init`
   - `make bookings-init`
   - `make ddl-gp`
2. Открыть Airflow UI (`http://localhost:8080`) и включить DAG `bookings_to_gp_stage`.
3. Вызвать `Trigger` DAG с конкретной датой (например, `2017-01-01` → зависит от стартовой конфигурации демобазы).
4. Посмотреть:
   - в `bookings-db` ― что появился день с бронированиями;
   - в Greenplum (`make gp-psql`) — данные в `stg.bookings`:
     - `SELECT * FROM stg.bookings LIMIT 10;`
     - `SELECT src_created_at_ts, load_dttm, batch_id FROM stg.bookings ORDER BY src_created_at_ts DESC LIMIT 10;`
5. Перезапустить DAG для следующей даты и увидеть, что:
   - генерация в `bookings.bookings` идёт по одному дню вперёд;
   - в `stg.bookings` появляются только новые записи (delta).

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

