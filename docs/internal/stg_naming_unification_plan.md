# Унификация нейминга служебных полей в STG

## Контекст

В STG-слое используются legacy-имена служебных полей (`batch_id`, `load_dttm`, `src_created_at_ts`), а начиная с ODS — каноничные (`_load_id`, `_load_ts`, `event_ts`). Студент видит разные имена для одного понятия. Цель — привести STG к канону, убрав расхождение.

> **Breaking change (dev-only).** Это ломающее переименование колонок. Миграционный шаг (ALTER TABLE … RENAME COLUMN) не предусмотрен. DDL-файлы используют `CREATE TABLE IF NOT EXISTS`, поэтому сами по себе они не пересоздадут существующие таблицы с новыми именами колонок. План предполагает заранее пересозданную среду (например, `make down && make up`) или ручной `DROP TABLE` / `DROP SCHEMA` перед `make ddl-gp`. Обратная совместимость не обеспечивается.

## Маппинг

| Legacy (STG сейчас) | Канон (ODS/DDS/DM) |
|---|---|
| `batch_id` | `_load_id` |
| `load_dttm` | `_load_ts` |
| `src_created_at_ts` | `event_ts` |

### Оговорка про `event_ts` в snapshot-справочниках

В транзакционных STG-таблицах (bookings, tickets, flights, segments, boarding_passes) поле `src_created_at_ts` действительно хранит время события из источника — переименование в `event_ts` семантически точно.

В snapshot-справочниках (airports, airplanes, routes, seats) это поле заполняется `now()` при загрузке, т.е. по факту это ещё одно load-time, а не время события. Тем не менее мы сохраняем единое имя `event_ts` как **учебное упрощение** — ради консистентной структуры STG-таблиц. Это зафиксировано как осознанный trade-off: единообразие важнее семантической точности в справочниках. В `naming_conventions.md` нужно добавить соответствующую оговорку (раздел 4, «Time Rule»).

## Что НЕ переименовываем

- PL/pgSQL переменная `v_batch_id` — это локальная переменная, не колонка
- Python-функция `_resolve_stg_batch_id`, переменная `stg_batch_id`, task_id `resolve_stg_batch_id` — это Python/Airflow-идентификаторы
- XCom-ключи, ссылающиеся на task_id

## Порядок выполнения

### Шаг 1: STG DDL (9 файлов)

`sql/stg/{bookings,tickets,flights,segments,airports,airplanes,routes,seats,boarding_passes}_ddl.sql`

В каждом: `batch_id` → `_load_id`, `load_dttm` → `_load_ts`, `src_created_at_ts` → `event_ts`.

### Шаг 2: STG Load (9 файлов)

`sql/stg/{bookings,tickets,flights,segments,airports,airplanes,routes,seats,boarding_passes}_load.sql`

INSERT-списки, SELECT, WHERE, комментарии — те же 3 замены.

### Шаг 3: STG DQ (9 файлов)

`sql/stg/{bookings,tickets,flights,segments,airports,airplanes,routes,seats,boarding_passes}_dq.sql`

WHERE-условия (`batch_id = v_batch_id` → `_load_id = v_batch_id`), RAISE-сообщения, комментарии.

### Шаг 4: ODS Load (9 файлов)

Два подтипа — обрабатывать по-разному.

#### 4a: Транзакционные таблицы (5 файлов)

`sql/ods/{bookings,tickets,flights,segments,boarding_passes}_load.sql`

SELECT из STG: `s.batch_id` → `s._load_id`, `s.load_dttm` → `s._load_ts`, `s.src_created_at_ts` → `s.event_ts`. Убрать лишние алиасы (`s.src_created_at_ts AS event_ts` → просто `s.event_ts`).

#### 4b: Snapshot-справочники (4 файла)

`sql/ods/{airports,airplanes,routes,seats}_load.sql`

Здесь `event_ts` отсутствует в целевой ODS-таблице — менять только ссылки на STG-колонки: `s.batch_id` → `s._load_id`, `s.load_dttm` → `s._load_ts`, `s.src_created_at_ts` → `s.event_ts` (только в ORDER BY / WHERE, где они читают из STG). ODS-колонка `_load_ts` по-прежнему заполняется через `now()`, это не меняется.

### Шаг 5: ODS DQ (9 файлов)

`sql/ods/{bookings,tickets,flights,segments,airports,airplanes,routes,seats,boarding_passes}_dq.sql`

`WHERE batch_id =` → `WHERE _load_id =`, RAISE-сообщения.

### Шаг 6: DAG-файлы (2 файла)

- `airflow/dags/bookings_to_gp_stage.py` — комментарий про `batch_id`
- `airflow/dags/bookings_to_gp_ods.py` — встроенный SQL-запрос резолвера: все `batch_id` как колонка → `_load_id`, `load_dttm` → `_load_ts`. Python-имена не трогаем.

### Шаг 7: Тесты (3 файла)

- `tests/test_ods_snapshot_integration.py` — inline DDL и INSERT в тестах
- `tests/test_dags_smoke.py` — комментарии
- `tests/test_ods_sql_contract.py` — docstring

### Шаг 8: Документация (~15 файлов)

- `docs/internal/naming_conventions.md` — убрать legacy-исключение (секция 5/STG), убрать переходный маппинг (секция 6), добавить оговорку про `event_ts` в snapshot-справочниках (секция 4)
- `docs/internal/db_schema.md` — описания STG-полей
- `docs/internal/bookings_stg_design.md` — дизайн STG
- `docs/internal/bookings_ods_design.md` — маппинг STG→ODS, SQL-примеры
- `docs/internal/qa-plan.md` — SQL-запросы проверок
- `docs/internal/architecture_review.md` — архитектурные заметки
- `docs/internal/bookings_stg_code_review.md` — код-ревью
- `docs/bookings_to_gp_stage.md` — описание STG DAG, примеры полей
- `docs/bookings_to_gp_ods.md` — описание ODS DAG
- `docs/dag_execution_order.md` — порядок выполнения DAG
- `educational-tasks.md` — учебные задания
- `README.md` — SQL-примеры в README
- `TESTING.md` — чек-лист тестирования

### Шаг 9: Верификация

```bash
# 1. Проверить SQL и Python — не должно быть колонок batch_id
#    (допустимы только: v_batch_id, stg_batch_id, resolve_stg_batch_id)
grep -rn 'batch_id' sql/stg/ sql/ods/ airflow/dags/ tests/ --include='*.sql' --include='*.py'

# 2. Ноль совпадений по старым именам в коде
grep -rn 'load_dttm' sql/ airflow/dags/ tests/ --include='*.sql' --include='*.py'
grep -rn 'src_created_at_ts' sql/ airflow/dags/ tests/ --include='*.sql' --include='*.py'

# 3. Проверить документацию — не должно быть старых имён как актуальных
#    (допустимы упоминания в историческом контексте)
grep -rn 'batch_id\|load_dttm\|src_created_at_ts' docs/ educational-tasks.md README.md TESTING.md

# 4. Тесты и линтер
make test
make fmt && make lint
```

## Итого: ~55 файлов, ~3 механические замены в каждом
