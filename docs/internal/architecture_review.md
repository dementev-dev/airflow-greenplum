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

- [x] **ODS batch resolver теряет данные при двух STG-запусках подряд**
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

- [ ] **Явный storage type для всех таблиц + AO где возможно** ✅ РЕШЕНИЕ ПРИНЯТО
  - 18 из 28 таблиц имеют неявный heap (нет `WITH`) — студент не видит, что выбор сделан
  - **Целевая раскладка по storage:**
    - **AO Column Store**: `dds.dim_calendar` (write-once, generate_series)
    - **AO Row + zlib**: ODS snapshot-справочники (`airports`, `airplanes`, `routes`, `seats`)
      — перевести загрузку с UPSERT на TRUNCATE+INSERT (честнее для full snapshot семантики)
    - **AO Row + zlib**: `dds.dim_tariffs` (только INSERT, нет UPDATE)
    - **AO Row + zlib**: `dm.route_performance` (full rebuild, по дизайну)
    - **Heap (явный)**: ODS транзакционные (`bookings`, `tickets`, `flights`, `segments`,
      `boarding_passes`) — row-level UPDATE при SCD1 UPSERT
    - **Heap (явный)**: DDS измерения с UPDATE (`dim_airports`, `dim_airplanes`,
      `dim_passengers`, `dim_routes`) и `fact_flight_sales`
    - **Heap (явный)**: DM витрины с UPSERT (`sales_report` и будущие HWM-витрины)
  - К каждой таблице добавить комментарий, объясняющий выбор storage type
  - Файлы: все `*_ddl.sql` в ods/, dds/, dm/ + переписать 4 ODS snapshot load-скрипта
  - См. ADR-3

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

## ПРИНЯТЫЕ АРХИТЕКТУРНЫЕ РЕШЕНИЯ

### ADR-1: Города, страны, модели самолётов — атрибуты измерений, не отдельные справочники

**Рассматривалось**: выделить `dim_city`, `dim_country`, `dim_airplane_model` как отдельные
измерения со своими суррогатными ключами.

**Решение**: оставить `city`, `country` как атрибуты `dim_airports`, а `model` — как атрибут
`dim_airplanes`. Не создавать отдельные справочники.

**Обоснование**:
1. **Star vs Snowflake.** Kimball-методология рекомендует «wide and flat» измерения.
   Вынос атрибутов в подтаблицы превращает star schema в snowflake — добавляет 2-3 JOIN-а
   в каждый запрос к факту без аналитического выигрыша. Для учебного стенда star schema —
   правильный эталон.
2. **Нет самостоятельной сущности в домене.** Город — JSON-атрибут аэропорта в источнике
   (`airport_name::json->>'ru'`). У него нет своего бизнес-ключа, жизненного цикла,
   независимых атрибутов. Модель самолёта — аналогично.
3. **Когнитивная нагрузка.** Лестница ODS→DDS уже крутая (6 измерений + 1 факт + SCD2).
   Добавление 2-3 измерений усложнит стенд без пропорционального обучающего эффекта.

**Когда отдельное измерение оправдано** (для справки студентам):
- Город имеет собственные атрибуты из другого источника (население, регион, координаты)
  → `dim_geography` как outrigger-измерение
- Модель самолёта имеет независимые характеристики (производитель, сертификация, конфигурации)
  → `dim_aircraft_type`
- В Data Vault — `hub_city` / `hub_country` как самостоятельные бизнес-объекты (другая парадигма)

### ADR-2: Heap + UPSERT вместо AO + партиционирование + exchange partition

**Контекст**: Greenplum широко распространён в РФ — на него активно мигрировали при
импортозамещении с Teradata и Exadata. Именно поэтому GP выбран для курсовой: опыт работы
с ним будет напрямую релевантен первой работе студента. Тем важнее, чтобы студенты понимали,
как устроены реальные GP-хранилища, даже если стенд использует упрощённый подход.

**Рассматривалось**: использовать production-паттерн крупных GP-хранилищ:
- AO Column Store (сжатие zlib/zstd, векторное чтение, колоночное хранение)
- Range-партиционирование по дате (`PARTITION BY RANGE (flight_date)`)
- Обновление через замену партиций (`ALTER TABLE EXCHANGE PARTITION`) или
  `DELETE + INSERT` в рамках одной партиции вместо row-level UPDATE

**Решение**: партиционирование не применяем (учебные объёмы). Для storage —
дифференцированный подход: heap для таблиц с UPDATE, AO для иммутабельных
(см. ADR-3 с полной раскладкой).

**Обоснование**:
1. **Универсальность паттерна.** UPSERT через UPDATE + INSERT работает в PostgreSQL,
   Snowflake, BigQuery, Redshift — везде. Exchange partition — GP-специфика
   (`ALTER TABLE ... EXCHANGE PARTITION FOR (...) WITH TABLE tmp_...`).
   Студент, освоив UPSERT, сможет применить его на любой платформе.
2. **Объём данных.** На учебных ~100K строк партиционирование не даёт partition pruning
   эффекта, зато утраивает DDL (стратегия, sub-partitions, retention policy).
   Выигрыш нулевой, когнитивная нагрузка — существенная.
3. **Простота ментальной модели.** «Вот строка, она обновилась» понятнее, чем «вот партиция,
   она заменилась целиком». Второй паттерн требует понимания storage engine, что выходит
   за рамки первого курса DWH.

**Что студенту важно знать про реальный GP** (для менти):

На продакшн-хранилищах с десятками и сотнями миллионов строк подход меняется принципиально:

| Аспект | Стенд (учебный) | Продакшн (реальный GP) |
|--------|-----------------|----------------------|
| Хранение фактов | Heap (row-oriented) | AO Column Store (сжатие, колонки) |
| Партиционирование | Нет | Range по дате (день/месяц) |
| Обновление | Row-level UPDATE | Exchange partition или DELETE+INSERT в партиции |
| Причина | UPDATE на AO «раздувает» таблицу (помечает строки deleted, дописывает новые) | |
| Когда переходить | > 10M строк, или когда VACUUM не справляется | |

Типичный production-паттерн загрузки факта по дням:
```sql
-- 1. Собрать новую партицию во временную таблицу
CREATE TABLE tmp_fact_20170102 (LIKE dds.fact_flight_sales)
    WITH (appendonly=true, orientation=column, compresstype=zlib);
INSERT INTO tmp_fact_20170102 SELECT ... FROM ods... WHERE flight_date = '2017-01-02';

-- 2. Атомарно заменить партицию (без DELETE, без UPDATE)
ALTER TABLE dds.fact_flight_sales
    EXCHANGE PARTITION FOR ('2017-01-02') WITH TABLE tmp_fact_20170102;

-- 3. Удалить временную таблицу (теперь в ней старые данные)
DROP TABLE tmp_fact_20170102;
```

Преимущества exchange partition:
- Нет row-level UPDATE → нет bloat, не нужен VACUUM
- AO Column Store даёт 5-10x сжатие и быстрые аналитические скана
- Partition pruning: запрос `WHERE flight_date = '2017-01-02'` читает только одну партицию
- Атомарность: EXCHANGE — одна DDL-команда, нет окна неконсистентности

### ADR-3: Явный storage type для каждой таблицы + AO где нет UPDATE

**Проблема**: 18 из 28 таблиц в ODS/DDS/DM создаются без `WITH`-клаузы. GP по умолчанию
создаёт heap, но студент не видит осознанного выбора — таблица «просто создаётся».
В учебном стенде каждое решение должно быть видимым и объяснённым.

**Решение**: добавить явный `WITH (...)` ко всем таблицам. Где row-level UPDATE не нужен —
перевести на AO (Row или Column) с компрессией.

**Целевая раскладка storage по таблицам:**

| Storage | Таблицы | Почему |
|---------|---------|--------|
| **AO Column** zlib | `dds.dim_calendar` | Write-once (generate_series), никогда не обновляется. Колоночное хранение идеально для аналитических скан. |
| **AO Column** zlib | `dm.route_performance` | Full rebuild (TRUNCATE+INSERT), чисто аналитические чтения. |
| **AO Row** zlib | STG: все 9 таблиц | Уже реализовано. Append-only, иммутабельные батчи. |
| **AO Row** zlib | ODS snapshot: `airports`, `airplanes`, `routes`, `seats` | Полный snapshot каждый раз. Перевести загрузку с UPSERT на TRUNCATE+INSERT — честнее для семантики «текущий срез». |
| **AO Row** zlib | `dds.dim_tariffs` | Только INSERT новых тарифов, UPDATE не используется. |
| **Heap** (явный) | ODS транзакционные: `bookings`, `tickets`, `flights`, `segments`, `boarding_passes` | Row-level UPDATE при SCD1 UPSERT. Heap обязателен. |
| **Heap** (явный) | DDS измерения с UPDATE: `dim_airports`, `dim_airplanes`, `dim_passengers`, `dim_routes` | SCD1/SCD2 UPSERT с row-level UPDATE. |
| **Heap** (явный) | `dds.fact_flight_sales` | UPDATE (is_boarded, seat_no меняются). |
| **Heap** (явный) | DM витрины с UPSERT: `sales_report` и будущие HWM-витрины | Row-level UPDATE при инкрементальном UPSERT. |

**Учебная ценность**: студенты видят на практике три storage-стратегии в одном проекте:
1. AO Column — для иммутабельных аналитических таблиц (dim_calendar, route_performance)
2. AO Row — для append-only данных и snapshot-справочников (STG, ODS refs, dim_tariffs)
3. Heap — для таблиц с row-level UPDATE (ODS транзакции, DDS dims с UPSERT, факт, DM)

И понимают **почему** выбор именно такой: UPDATE на AO = bloat + необходимость VACUUM.

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
| ~~P1~~ | ~~Добавить 7 точечных комментариев~~ | ~~30-40 мин~~ | ~~done~~ |
| **P2** | Явный storage type + AO где нет UPDATE (ADR-3) | 2-3 часа | все `*_ddl.sql` в ods/dds/dm + 4 ODS snapshot load |
| **P2** | Рефакторинг hashdiff → TEMP TABLE | 1 час | `sql/dds/dim_routes_load.sql` |
| **P2** | Переименовать STG поля в канон + заметка | 1-2 часа | 27 STG SQL + ODS load + naming_conventions.md |
| **P2** | TEMP TABLE для сложных ODS load-ов | 1 час | 3-4 ODS load файла |
| **P2** | Реализовать `dm.route_performance` | 2-3 часа | 3 SQL + DAG + тесты |
| **P3** | Маршрут изучения DDS + distribution strategy doc | 1 час | 2 новых md-файла |
