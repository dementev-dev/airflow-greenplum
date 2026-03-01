# Ревью архитектуры слоёв DWH: оценка учебной ценности

> Дата: 2026-03-01
> Статус: backlog задач для доработки
> Контекст: оценка текущей конструкции слоёв с точки зрения учебных целей

## Context

Стенд — курсовая работа и эталон для менти-джунов. Они понесут эти паттерны на свою первую работу. Оцениваем по двум осям: **production-ready** (чтобы не стыдно было показать на собеседовании) и **KISS** (чтобы джун не утонул в сложности).

Текущее состояние: 95 SQL-файлов, 5 слоёв (STG→ODS→DDS→DM), 9 DAG-ов, полная Star Schema с SCD2, DQ на каждом шаге. Реализована 1 из 5 витрин DM.

---

## СИЛЬНЫЕ СТОРОНЫ (что уже отлично)

### 1. Паттерн load → DQ на каждом шаге — эталонный
Каждая сущность в каждом слое имеет тройку файлов `_ddl.sql` / `_load.sql` / `_dq.sql`. DAG-и обеспечивают порядок load→dq→next. Smoke-тесты проверяют рёбра графа. Студенты усвоят: **DQ — не опция, а часть пайплайна**.

### 2. UPSERT через UPDATE + INSERT — production-grade для Greenplum
Не DELETE+INSERT (дорого на AO-таблицах), не MERGE (нет в GP6). `IS DISTINCT FROM` для null-safe сравнения — деталь, которую даже опытные инженеры забывают.

### 3. Антипаттерн-обучение в DM (distribution by date)
Комментарий в `dm/sales_report_ddl.sql` объясняет **почему** нельзя распределять по дате, с конкретными причинами (Load Skew, Processing Skew). Это «почему нет» — именно то, что не дают учебники.

### 4. SCD2 в dim_routes — полный и корректный
Все три кейса: закрытие изменённых версий (hashdiff), закрытие исчезнувших маршрутов, вставка новых версий с правильной логикой valid_from. DQ проверяет пересечение интервалов. Готовый reference implementation.

### 5. Point-in-time lookup в факте — ключевой навык
```sql
LEFT JOIN dds.dim_routes AS rte
    ON rte.route_bk = flt.route_no
    AND flt.scheduled_departure::DATE >= rte.valid_from
    AND (rte.valid_to IS NULL OR flt.scheduled_departure::DATE < rte.valid_to)
```
Многие продакшн-DWH ошибаются, присоединяя только текущую версию.

### 6. HWM-инкрементальность в DM — самовосстанавливающийся пайплайн
`MAX(_load_ts)` + TEMP TABLE для однократной агрегации — канон MPP. Пайплайн сам «догоняет» пропущенные дни.

### 7. DAG-графы корректно отражают зависимости данных
Параллельность airports/airplanes, gates на routes (нужны оба), факт после всех измерений. Smoke-тесты проверяют и наличие, и **отсутствие** рёбер (параллельность).

### 8. ANALYZE после каждой загрузки
GP-специфичная best practice, которую забывают даже опытные команды.

### 9. Идемпотентные STG-загрузки
`NOT EXISTS (... WHERE batch_id = '{{ run_id }}')` — простой, корректный, понятный паттерн для retry-safe загрузок.

---

## ЗАМЕЧАНИЯ И ЗАДАЧИ ДЛЯ ДОРАБОТКИ

### P0: Фактическая ошибка (исправить до показа студентам)

- [ ] **ODS batch resolver теряет данные при двух STG-запусках подряд**
  - Сценарий: STG run_1 загружает день N, STG run_2 загружает день N+1, затем ODS запускается
  - `_resolve_stg_batch_id()` выбирает только последний согласованный batch (`run_2`)
  - Все ODS load-скрипты фильтруют `WHERE batch_id = 'run_2'` → данные `run_1` навсегда пропущены
  - **Справочники** (airports, airplanes, routes, seats): проблемы нет — full snapshot, `run_2` содержит всё
  - **Транзакционные таблицы** (bookings, tickets, flights, segments, boarding_passes): **потеря данных** — инкрементальные записи `run_1` никогда не попадут в ODS
  - Корень проблемы: batch resolver проектировался для согласованности справочников (INTERSECT), но тот же single-batch фильтр применяется к транзакционным таблицам, где нужны **все необработанные** batch-и
  - **Нужно**: разделить логику — для транзакционных таблиц загружать все batch-и с `_load_ts > MAX(_load_ts в ODS)` (аналог HWM из DM), для справочников — по-прежнему последний согласованный
  - Файлы: `airflow/dags/bookings_to_gp_ods.py`, `sql/ods/bookings_load.sql`, `sql/ods/tickets_load.sql`, `sql/ods/flights_load.sql`, `sql/ods/segments_load.sql`, `sql/ods/boarding_passes_load.sql`

- [x] **Противоречие в distribution key для airport_traffic**
  - `bookings_dm_design.md` (строка 182): `DISTRIBUTED BY (traffic_date)`
  - `sales_report_ddl.sql`: явно объясняет, почему distribution by date — антипаттерн
  - **Нужно**: исправить на `DISTRIBUTED BY (airport_sk)` в дизайн-документе
  - Файл: `docs/internal/bookings_dm_design.md`

### P1: Высокий эффект, минимум усилий (комментарии и документация)

- [x] **Нет объяснения «почему не SERIAL» в генерации SK**
  - `MAX(sk) + ROW_NUMBER()` корректен для GP, но студент на PostgreSQL/Snowflake будет использовать `IDENTITY`/`SEQUENCE`
  - **Нужно**: 4-строчный комментарий в `sql/dds/dim_airports_load.sql`

- [x] **Факт без суррогатного ключа — не объяснено «почему»**
  - Натуральный (ticket_no, flight_id) как grain — правильное Kimball-моделирование
  - **Нужно**: комментарий в `sql/dds/fact_flight_sales_ddl.sql`

- [x] **Нет упоминания cross-DAG зависимостей**
  - STG, ODS, DDS, DM — отдельные DAG-и с `schedule=None`, студент может не понять порядок
  - **Нужно**: комментарий в docstring каждого DAG или `docs/dag_execution_order.md`

- [x] **Late-arriving dimensions не упомянуты**
  - Факт делает LEFT JOIN → `passenger_sk = NULL` при опоздании; нет механизма исправления
  - **Нужно**: комментарий в `sql/dds/fact_flight_sales_load.sql` у LEFT JOIN-ов

- [x] **Batch resolver недообъяснён**
  - `_resolve_stg_batch_id` с INTERSECT по 4 таблицам — нет комментария **зачем** нужна согласованность
  - **Нужно**: комментарий в `airflow/dags/bookings_to_gp_ods.py` перед SQL-запросом

- [x] **`helpers/greenplum.py` без пометки «legacy»**
  - Использует прямой psycopg2 + ENV — противоречит PostgresOperator-подходу
  - **Нужно**: docstring «LEGACY: только для CSV-пайплайна» в `airflow/dags/helpers/greenplum.py`

### P2: Средние усилия, заметное улучшение качества

- [ ] **Дублирование hashdiff CTE в dim_routes_load.sql**
  - `md5(COALESCE(...))` повторяется в Statement 1 и Statement 2, ROW_NUMBER() — 3 раза
  - **Решение**: вынести в `CREATE TEMP TABLE tmp_routes_src ON COMMIT DROP`
  - Файл: `sql/dds/dim_routes_load.sql`

- [ ] **Несогласованность нейминга STG vs ODS+** ✅ РЕШЕНИЕ ПРИНЯТО
  - STG: `batch_id`, `load_dttm`, `src_created_at_ts` → переименовать в канон `_load_id`, `_load_ts`, `event_ts`
  - Единый словарь во всех слоях снижает когнитивную нагрузку
  - Добавить заметку в `naming_conventions.md` (секция «legacy-нейминг в реальных проектах»)
  - Удалить секцию 6 «Переходный маппинг» как неактуальную
  - Файлы: ~27 STG SQL + ODS load-скрипты + `naming_conventions.md` + тесты

- [ ] **Дублирование CTE в ODS load-скриптах**
  - `WITH src AS (...)` копируется 2-3 раза в каждом из 9 ODS load-файлов
  - **Решение**: TEMP TABLE для самых сложных (airports, flights, routes); простые — оставить
  - Файлы: `sql/ods/airports_load.sql`, `sql/ods/flights_load.sql`, `sql/ods/routes_load.sql`

- [ ] **DM слой незавершён**
  - 1 из 5 витрин реализована, остальные — закомментированные заглушки
  - **Решение**: реализовать `route_performance` (full rebuild + AO Column Store); остальные 3 — задания для студентов
  - Файлы: `sql/dm/route_performance_*.sql` (новые), DAG, тесты

### P3: Хорошо бы, но не горит

- [ ] **Крутая лестница сложности ODS→DDS**
  - **Решение**: создать «маршрут изучения» DDS: tariffs → calendar → airports → passengers → routes (SCD2) → fact

- [ ] **DQ без переиспользуемых функций**
  - **Решение**: добавить один опциональный пример `sql/lib/dq_assert_no_duplicates()` как seed

- [ ] **Нет документа по стратегии distribution**
  - **Решение**: `docs/internal/distribution_strategy.md` с объяснением логики для каждого слоя

- [ ] **Отсутствующие паттерны** (комментарии/заметки):
  - Partitioning (когда и зачем, почему не здесь)
  - SCD Type 3/6 (хотя бы упомянуть существование)
  - Data lineage (`_load_id` в DM ≠ `batch_id` в STG — нет сквозного трассирования)

---

## ЧТО ОСТАВИТЬ КАК ЕСТЬ

| Аспект | Почему не трогаем |
|--------|-------------------|
| Факт без SK | Правильное моделирование, нужен только комментарий (P1) |
| ODS DAG с 20 задачами | Не перегружает — параллельная структура наглядна на графе |
| 5 витрин DM | Правильное количество, каждая учит своему паттерну |
| PL/pgSQL DQ | Достаточно для учебного проекта, фреймворк — перебор |
| MAX+ROW_NUMBER для SK | Корректно для GP, нужен только комментарий (P1) |

---

## Сводка по трудозатратам

| Приоритет | Действие | Оценка | Ключевые файлы |
|-----------|----------|--------|----------------|
| **P0** | ODS batch resolver: разделить логику для справочников и транзакций | 2-3 часа | ODS DAG + 5 транзакционных load-скриптов |
| ~~P0~~ | ~~Исправить distribution key в airport_traffic~~ | ~~5 мин~~ | ~~done~~ |
| **P1** | Добавить 7 точечных комментариев | 30-40 мин | 6-7 файлов (DDL, load, DAG) |
| **P2** | Рефакторинг hashdiff → TEMP TABLE | 1 час | `sql/dds/dim_routes_load.sql` |
| **P2** | Переименовать STG поля в канон + заметка | 1-2 часа | 27 STG SQL + ODS load + naming_conventions.md |
| **P2** | TEMP TABLE для сложных ODS load-ов | 1 час | 3-4 ODS load файла |
| **P2** | Реализовать `dm.route_performance` | 2-3 часа | 3 SQL + DAG + тесты |
| **P3** | Маршрут изучения DDS + distribution strategy doc | 1 час | 2 новых md-файла |
