# Plan: Перенос STG в эталон, ODS airplanes+seats — студенту

> Версия: 4 (после третьего ревью ChatGPT 5.4)
> Дата: 2026-03-11

## Контекст и мотивация

### Обнаруженная проблема

При подготовке к Этапу 4 (раскладка по веткам main / solution) обнаружен **конфликт
между текущим дизайном БД** (`docs/design/db_schema.md`, `docs/design/bookings_dds_design.md`)
**и планом курсового задания** (`docs/design/assignment_design.md`).

Суть конфликта: эталонный пайплайн (который должен работать «из коробки» на main-ветке
для студента) **неработоспособен**, если студент ещё не реализовал ни одного задания.
Три DDS-измерения (`dim_routes`, `dim_passengers`, `dim_airplanes`) отнесены к заданию
студента, но эталонная фактовая таблица `fact_flight_sales` зависит от них через
цепочку LEFT JOIN. Без этих измерений факт загружается с массовыми NULL в ключевых FK,
а DQ-проверки блокируют pipeline. Эталонные витрины (`sales_report`, `airport_traffic`)
становятся бесполезными.

Проблема усугубляется архитектурой batch resolver ODS DAG и тестовыми контрактами,
которые требуют данных во **всех** snapshot-таблицах STG.

Ниже — детальный разбор и выбранное решение.

### Фактовая таблица: зависимость от студенческих измерений

`fact_flight_sales` зависит от **всех 7 измерений** DDS,
включая 3 студенческих: `dim_routes`, `dim_passengers`, `dim_airplanes`.

Критическая цепочка: аэропорты в факте разрешаются **через `dim_routes`**:

```
flt.route_no → dim_routes.route_bk → rte.departure_airport → dim_airports.airport_bk → airport_sk
```

Если `dim_routes` пуст (студент ещё не реализовал), то **NULL** получают не только
`route_sk`, но и оба `airport_sk` и `airplane_sk`. Эталонная витрина `sales_report`
становится бесполезной.

Причина: `ods.flights` содержит только `route_no`, но не `departure_airport` /
`arrival_airport` / `aircraft_code`. Эти поля доступны только через таблицу `routes`.

### Почему нельзя просто использовать DDL-заглушки

Если `dim_routes` — пустая заглушка, все LEFT JOIN через неё возвращают NULL.
Факт загружается, но:

- `departure_airport_sk = NULL` → `sales_report` не может группировать по аэропортам
- `arrival_airport_sk = NULL` → `airport_traffic` не может считать трафик
- UPDATE факта **не перезаписывает SK** (by design, строка 4-5 load-скрипта) →
  NULL-и остаются навсегда до TRUNCATE + перезагрузки

### Дополнительные архитектурные ограничения (из ревью)

1. **Fact DQ блокирует загрузку** (`fact_flight_sales_dq.sql`):
   - Строка 71: `passenger_sk IS NULL` → **hard fail** (100% NULL → exception)
   - Строка 98: `route_sk / airplane_sk IS NULL` → при >1% строк → **exception**
   - На main с пустыми студенческими dims — 100% NULL → pipeline падает

2. **Batch resolver ODS DAG** (`bookings_to_gp_ods.py:59-77`):
   - `INTERSECT` по `stg.airports`, `stg.airplanes`, `stg.routes`, `stg.seats`
   - Если **любая** STG-таблица пуста — общего batch нет → весь ODS pipeline падает
   - Контракт зафиксирован тестом `test_ods_sql_contract.py:30`

### Выбранное решение

**Весь STG-слой — эталонный** (все 9 таблиц загружаются reference-кодом).
Это гарантирует:
- Batch resolver всегда находит согласованный batch (все 4 snapshot-таблицы заполнены)
- DAG-зависимости и smoke-тесты STG не требуют изменений
- Никакого расхождения batch-контракта между main и solution

**ODS: `airplanes` + `seats` остаются заданием студента** (TRUNCATE+INSERT практика).

**DDS: `dim_airplanes`, `dim_passengers`, `dim_routes`** — задание студента (DDL-заглушки).

**Fact lookup**: аэропорты через `ods.routes` (эталон); `airplane_sk` остаётся
point-in-time через `dim_routes` (NULL на main — допустимо).

**Fact DQ**: на main ослабить проверки студенческих SK.

### Что теряет студент (и почему это приемлемо)

| Потеря | Компенсация |
|--------|-------------|
| STG-практика (PXF external tables) | Изучает эталонный STG-код; отдельный PXF-практикум в бэклоге (`assignment_design.md`, раздел 6) |
| `ods.routes` (TRUNCATE+INSERT) | Остаются `ods.airplanes` + `ods.seats` — тот же паттерн |

Ключевые элементы задания **сохранены**:
- ODS: 2 таблицы (airplanes, seats) — практика TRUNCATE+INSERT
- DDS: `dim_airplanes` (SCD1), `dim_passengers` (SCD1), **`dim_routes` (SCD2)** — ключевой вызов
- DM: 4 витрины разной сложности (от простой к сложной)

### Пересчёт факта после реализации студентом

После реализации всех измерений студенту нужно:
1. Запустить загрузку DDS-измерений (dim_airplanes, dim_passengers, dim_routes)
2. `TRUNCATE dds.fact_flight_sales;`
3. Перезапустить загрузку факта → теперь все SK заполнены
4. Перезапустить DM-витрины

Это стандартная практика при late-arriving dimensions и хорошая обучающая точка.

### airport_traffic — студенческое задание

`airport_traffic` архитектурно не зависит от студенческих dims (использует только
`calendar_sk`, `departure_airport_sk`, `arrival_airport_sk`). Но это **не значит**,
что витрина готова: SQL витрины пишет студент. Данные в факте есть (эталон),
студент пишет агрегирующий SQL. Статус: **задание студента** (как и остальные 3 DM).

---

## Матрица зависимостей DM-витрин

| DM витрина | calendar | airports | tariffs | routes | passengers | airplanes | Работает без студ. dims? |
|---|---|---|---|---|---|---|---|
| `sales_report` (эталон) | + | + | + | — | — | — | **ДА** |
| `airport_traffic` (студент) | + | + | — | — | — | — | Данные есть, SQL пишет студент |
| `route_performance` (студент) | + | — | — | **+** | — | — | Нет |
| `monthly_overview` (студент) | + | — | — | **+** | **+** | **+** | Нет |
| `passenger_loyalty` (студент) | + | — | + | **+** | **+** | — | Нет |

---

## Изменения (код)

### 1. `sql/dds/fact_flight_sales_load.sql` — lookup аэропортов через ods.routes

**Ветка:** chore/bookings-etl (сейчас) + main (Этап 4)

**Текущее состояние** (строки 51-62):
```sql
LEFT JOIN dds.dim_routes AS rte
    ON rte.route_bk = flt.route_no
    AND flt.scheduled_departure::DATE >= rte.valid_from
    AND (rte.valid_to IS NULL OR flt.scheduled_departure::DATE < rte.valid_to)
...
LEFT JOIN dds.dim_airports AS dep
    ON dep.airport_bk = rte.departure_airport       -- через dim_routes!
LEFT JOIN dds.dim_airports AS arr
    ON arr.airport_bk = rte.arrival_airport          -- через dim_routes!
LEFT JOIN dds.dim_airplanes AS ap
    ON ap.airplane_bk = rte.airplane_code            -- через dim_routes!
```

**После изменения:**
```sql
-- Учебный комментарий: Airport lookup через ods.routes (эталонный справочник),
-- а не через dds.dim_routes (студенческое задание SCD2).
-- Это архитектурное решение: эталонный пайплайн работает независимо от студенческого кода.
-- ROW_NUMBER по validity DESC: выбираем актуальную версию расписания маршрута.
-- Аэропорты вылета/прилёта одинаковы во всех версиях одного route_no.
LEFT JOIN (
    SELECT route_no, departure_airport, arrival_airport
    FROM (
        SELECT route_no, departure_airport, arrival_airport,
               ROW_NUMBER() OVER (PARTITION BY route_no ORDER BY validity DESC) AS rn
        FROM ods.routes
    ) ranked
    WHERE rn = 1
) AS ods_rte ON ods_rte.route_no = flt.route_no
LEFT JOIN dds.dim_routes AS rte
    ON rte.route_bk = flt.route_no
    AND flt.scheduled_departure::DATE >= rte.valid_from
    AND (rte.valid_to IS NULL OR flt.scheduled_departure::DATE < rte.valid_to)
...
LEFT JOIN dds.dim_airports AS dep
    ON dep.airport_bk = ods_rte.departure_airport    -- через ods.routes (эталон)
LEFT JOIN dds.dim_airports AS arr
    ON arr.airport_bk = ods_rte.arrival_airport      -- через ods.routes (эталон)
LEFT JOIN dds.dim_airplanes AS ap
    ON ap.airplane_bk = rte.airplane_code            -- через dim_routes (point-in-time, как прежде)
```

**Почему аэропорты через ods.routes, а airplane через dim_routes:**

- **Аэропорты**: `departure_airport` и `arrival_airport` одинаковы во всех версиях
  одного `route_no` (маршрут SVO→LED всегда SVO→LED). Безопасно брать из ODS.
  Это даёт эталонным витринам (`sales_report`, `airport_traffic`) корректные `airport_sk`
  даже без студенческого `dim_routes`.

- **Самолёт**: `airplane_code` теоретически может отличаться между версиями маршрута.
  Текущий контракт DDS (`bookings_dds_design.md:499`) фиксирует, что `airplane_sk`
  приходит из **той же point-in-time версии** маршрута, что и `route_sk`.
  Менять эту семантику — изменение модели данных, а не стабилизация.
  На main `airplane_sk` будет NULL (dim_routes — заглушка). Это допустимо:
  ни `sales_report`, ни `airport_traffic` не используют `airplane_sk`.

### 2. `sql/dds/fact_flight_sales_dq.sql` — ослабить проверки студенческих SK (только main)

**Ветка:** только main (Этап 4). На solution-ветке полные проверки сохраняются.

Изменения:

- **Строки 65-75** (`passenger_sk IS NULL`): заменить `RAISE EXCEPTION` на `RAISE NOTICE`.
  На main `dim_passengers` — заглушка, 100% строк будут с `passenger_sk IS NULL`.
  Любой порог (даже >1%) даст exception. **Только логирование, без блокировки.**

- **Строки 89-108** (route-related FK): разделить на два блока:

  **Блок A — эталонные FK (проверка с порогом 1%, как calendar_sk):**
  ```sql
  -- departure_airport_sk и arrival_airport_sk заполняются через ods.routes (эталон).
  -- NULL здесь — аномалия данных (пропущен маршрут в ODS), а не отсутствие студенческого кода.
  -- Порог 1% — защита от единичных аномалий источника (аналогично calendar_sk).
  SELECT COUNT(*) INTO v_null_airport
  FROM dds.fact_flight_sales
  WHERE departure_airport_sk IS NULL OR arrival_airport_sk IS NULL;

  IF v_null_airport > 0 THEN
      IF v_null_airport * 100.0 / NULLIF(v_row_count, 0) > 1.0 THEN
          RAISE EXCEPTION 'DQ FAILED: NULL airport_sk: % (>1%%)', v_null_airport;
      ELSE
          RAISE NOTICE 'DQ WARNING: NULL airport_sk: % (<=1%%, допустимо)', v_null_airport;
      END IF;
  END IF;
  ```

  **Блок B — студенческие FK (только логирование, RAISE NOTICE):**
  ```sql
  -- route_sk и airplane_sk будут NULL, пока студент не реализует dim_routes.
  -- Не блокируем pipeline.
  SELECT COUNT(*) INTO v_null_student
  FROM dds.fact_flight_sales
  WHERE route_sk IS NULL OR airplane_sk IS NULL;

  IF v_null_student > 0 THEN
      RAISE NOTICE 'DQ INFO: студенческие SK (route/airplane) NULL: %. '
          'После реализации dim_routes: TRUNCATE fact → перезагрузка.',
          v_null_student;
  END IF;
  ```

Итоговое правило на main:
- `departure_airport_sk`, `arrival_airport_sk` — **проверка с порогом 1%** (эталон, через ods.routes; >1% → EXCEPTION, <=1% → NOTICE)
- `passenger_sk`, `route_sk`, `airplane_sk` — **только RAISE NOTICE** (студенческие заглушки, 100% NULL допустимо)

```sql
-- Учебный комментарий: route_sk, airplane_sk и passenger_sk будут NULL,
-- пока вы не реализуете соответствующие DDS-измерения.
-- После реализации: TRUNCATE dds.fact_flight_sales → перезагрузка → все SK заполнены.
-- Полную версию DQ (с блокировкой) см. в ветке solution.
```

### 3. ODS Routes DQ: убрать RI-проверку airplane_code (только main)

**Ветка:** только main (Этап 4). На solution-ветке полные проверки сохраняются.

> **Почему только ODS, не STG?** Весь STG — эталон, `stg.airplanes` всегда заполнен,
> `stg/routes_dq.sql` RI-проверка к `stg.airplanes` проходит штатно. Менять STG не нужно.
> На уровне ODS `ods.airplanes` — студенческая заглушка (пустая), поэтому
> `ods/routes_dq.sql` RI-проверка к `ods.airplanes` упадёт.

**Файл:**
- `sql/ods/routes_dq.sql` — удалить блок строк 143-156 (RI к `ods.airplanes`)

Добавить комментарий:
```sql
-- Учебный комментарий: проверка RI airplane_code → ods.airplanes не выполняется,
-- т.к. таблица ods.airplanes реализуется студентом. После реализации — раскомментируйте
-- (см. ветку solution для полной версии).
```

### 4. ODS DAG-зависимость: убрать airplanes → routes (только main)

**Ветка:** только main (Этап 4)

> **Почему только ODS, не STG?** STG DAG не меняется: `stg.airplanes` — эталон,
> барьер `check_airplanes_dq >> load_routes_to_stg` работает штатно.
> На уровне ODS `dq_ods_airplanes` — заглушка (`SELECT 1;`), формально проходит,
> но зависимость вводит в заблуждение. Убираем для ясности.

`airflow/dags/bookings_to_gp_ods.py` (строка 266):
```python
# Было:
[dq_ods_airports, dq_ods_airplanes] >> load_ods_routes
# Стало:
dq_ods_airports >> load_ods_routes
```

### 5. Заглушки для студенческих файлов (только main)

**Ветка:** только main (Этап 4)

#### STG-слой: весь код остаётся (эталон), заглушки не нужны

Все 9 STG-таблиц загружаются эталонным кодом. Batch resolver работает без изменений.

#### ODS-слой: заглушки для airplanes + seats

| Файл | main (студент) | solution |
|------|----------------|----------|
| `sql/ods/airplanes_ddl.sql` | Полный DDL (таблица нужна) | Тот же |
| `sql/ods/airplanes_load.sql` | Заглушка: `SELECT 1; -- TODO` | Полная реализация |
| `sql/ods/airplanes_dq.sql` | Заглушка: `SELECT 1;` | Полная реализация |
| `sql/ods/seats_ddl.sql` | Полный DDL | Тот же |
| `sql/ods/seats_load.sql` | Заглушка | Полная реализация |
| `sql/ods/seats_dq.sql` | Заглушка | Полная реализация |

#### DDS-слой: заглушки для 3 измерений

| Файл | main (студент) | solution |
|------|----------------|----------|
| `sql/dds/dim_routes_ddl.sql` | Полный DDL (нужен для LEFT JOIN) | Тот же |
| `sql/dds/dim_routes_load.sql` | Заглушка: `SELECT 1; -- TODO: реализуйте SCD2` | Полная реализация |
| `sql/dds/dim_routes_dq.sql` | Заглушка: `SELECT 1;` | Полная реализация |
| `sql/dds/dim_passengers_ddl.sql` | Полный DDL | Тот же |
| `sql/dds/dim_passengers_load.sql` | Заглушка | Полная реализация |
| `sql/dds/dim_passengers_dq.sql` | Заглушка | Полная реализация |
| `sql/dds/dim_airplanes_ddl.sql` | Полный DDL | Тот же |
| `sql/dds/dim_airplanes_load.sql` | Заглушка | Полная реализация |
| `sql/dds/dim_airplanes_dq.sql` | Заглушка | Полная реализация |

#### DM-слой: заглушки для 4 витрин

Аналогично: DDL остаётся, load/dq заменяются заглушками.
Файлы: `airport_traffic`, `route_performance`, `monthly_overview`, `passenger_loyalty`.

---

## Изменения (документация)

### 6. `docs/assignment/analyst_spec.md`

- **Удалить** разделы 1.1-1.3 (все STG-задания) — весь STG теперь эталон
- **Удалить** разделы 2.3 (`ods.routes`) — routes теперь эталон
- **Обновить** рекомендуемый порядок: начинается с ODS (airplanes, seats)
- **Добавить** мотивационную секцию «Почему STG уже реализован»:
  - Эталонный пайплайн требует согласованного batch по всем snapshot-таблицам
  - Без данных в STG невозможна загрузка ODS и далее по цепочке
  - Студент изучает эталонный STG-код как образец
- **Добавить** секцию «Пересчёт факта после реализации измерений»:
  - Инструкция: TRUNCATE fact + re-run после реализации всех DDS-измерений
  - Педагогическая ценность: late-arriving dimensions, dependency management

### 7. `docs/design/assignment_design.md`

- **Обновить** таблицу «Эталонные таблицы»: добавить весь STG + ods.routes
- **Обновить** таблицу «Задание студенту»: убрать STG целиком, ODS оставить airplanes+seats
- **Обновить** рекомендуемый порядок: начинается с ODS
- **Обновить** структуру валидационного DAG (раздел 4): убрать `validate_stg` секцию

### 8. `docs/design/bookings_dds_design.md`

- **Строки 463, 468** (SQL-скелет факта): обновить JOIN-блок —
  `dep.airport_bk` и `arr.airport_bk` теперь через `ods_rte`, а не через `rte`;
  `ap.airplane_bk` остаётся через `rte` (point-in-time)
- **Строка 499-501** (текстовое пояснение): обновить —
  `departure_airport_sk` и `arrival_airport_sk` берутся из `ods.routes` (эталон);
  `airplane_sk` остаётся point-in-time через `dim_routes`
- **Строка 558** (passenger_sk = 0): убрать утверждение о нулевой толерантности —
  на main `passenger_sk` будет 100% NULL (dim_passengers — заглушка)
- **Строка 559** (DQ текстовое описание): обновить — на main student SK
  (`passenger_sk`, `route_sk`, `airplane_sk`) не блокируют pipeline;
  `airport_sk` проверяются с порогом 1%
- **Строка 741** (DQ SQL-пример): обновить встроенный SQL — разделить route-related
  блок на эталонный (airport_sk, порог 1%) и студенческий (route_sk, airplane_sk, NOTICE)

### 9. `docs/design/db_schema.md`

- Пометить весь STG и `ods.routes` как эталонные (не студенческие)
- Строка 3: «Все слои реализованы» — уточнить, что на main ODS airplanes/seats,
  DDS student dims и DM student vitrines — заглушки
- Строка 253: DQ-контракт «SQL-скрипты с RAISE EXCEPTION» — добавить оговорку,
  что на main студенческие DQ-скрипты являются заглушками

### 10. Тесты

- `tests/test_dags_smoke.py` строки 242-243:
  На main убрать assert барьера `dq_ods_airplanes → dq_ods_routes` (ODS routes
  не зависит от ODS airplanes на main). STG-барьеры (строки 123-124) **не трогаем**:
  STG целиком эталонный, барьер `check_airplanes_dq → check_routes_dq` работает.
- `tests/test_ods_sql_contract.py` строка 6:
  На main изменить `SNAPSHOT_ENTITIES` — исключить студенческие ODS:
  ```python
  # Было:
  SNAPSHOT_ENTITIES = ("airports", "airplanes", "routes", "seats")
  # Стало (main):
  SNAPSHOT_ENTITIES = ("airports", "routes")
  ```
  Тесты `test_snapshot_load_scripts_use_truncate` (строка 13) и
  `test_snapshot_dq_checks_extra_keys` (строка 22) проверяют паттерны TRUNCATE+INSERT
  и `v_extra_keys_count` в SQL-файлах. Заглушки `SELECT 1;` не содержат этих паттернов →
  тесты упадут для airplanes/seats. Исключение из `SNAPSHOT_ENTITIES` решает проблему.
  Batch resolver тест (строка 30) проверяет DAG-код (не SQL-файлы) — без изменений.

### 11. Прочие документы

- `docs/design/PRD.md` — обновить чеклист (раздел 10)
- `docs/design/bookings_ods_design.md` — комплексное обновление:
  - Строка 29, 515, 534: пометить `airplanes` и `seats` как студенческие
    (на main — заглушки; STG-уровень — эталон, ODS load/dq — студент)
  - Строки 521, 523: скорректировать общий DQ-контракт — на main `airplanes_dq.sql`
    и `seats_dq.sql` являются заглушками, а не полноценными RAISE EXCEPTION скриптами
  - Строка 620: обновить DAG-граф — routes больше не зависит от airplanes на ODS
  - Строка 634: убрать зависимость routes от airplanes в описании FK
- `docs/bookings_to_gp_ods.md` — обновить описание
- `docs/bookings_to_gp_dds.md`:
  - Строки 111-113: убрать гарантию отсутствия NULL SK в штатном режиме —
    на main student SK (`passenger_sk`, `route_sk`, `airplane_sk`) будут NULL
  - Строка 116: обновить DQ-семантику — student SK не блокируют (NOTICE);
    airport_sk — порог 1%
  - Строка 126: обновить описание lookup (airports через ods.routes)
- `docs/reference/qa-plan.md`:
  - Строка 80: «ODS: все 9 таблиц не пустые» — на main `ods.airplanes` и `ods.seats`
    будут пустыми by design (студенческие заглушки). Уточнить формулировку.
- `docs/e2e-etl-test-protocol.md` — обновить контекст

---

## Порядок выполнения

### Сейчас (на chore/bookings-etl)

1. **Код**: изменить `fact_flight_sales_load.sql` (lookup через ods.routes) — п.1
2. **Документация**: обновить `analyst_spec.md` — п.6
3. **Документация**: обновить `assignment_design.md` — п.7
4. **Документация**: обновить `db_schema.md` — п.9
5. **Тестирование**: `make test` + ручная проверка SQL
6. **Коммит**

### Этап 3 (валидационный DAG)

- Проектировать с учётом DDL-заглушек
- `validate_stg` секцию **не делать** (STG — эталон)
- `check_dim_routes_scd2` — активный тест (мутация ODS → проверка → откат)

### Этап 4 (подготовка main + solution)

- Fact DQ: ослабить проверки student SK (main only) — п.2
- Routes DQ: убрать airplane RI (main only) — п.3
- DAG-зависимости: убрать airplanes → routes (main only) — п.4
- Заглушки: ODS (airplanes, seats) + DDS (3 dims) + DM (4 vitrines) — п.5
- Тесты: обновить smoke-тесты (main only) — п.10
- Документация: bookings_dds_design, PRD, прочее — п.8, п.11

---

## Верификация

### После п.1 (fact_flight_sales fix)

```bash
make test                         # smoke-тесты DAG
```

Ручная проверка (при поднятом стенде):

> **Важно:** UPDATE факта не перезаписывает dimension SK (by design, строка 4-5).
> Уже загруженные строки сохранят старые значения `airport_sk` (через dim_routes).
> Для полной проверки новой логики нужен **TRUNCATE + перезагрузка**:

```sql
TRUNCATE dds.fact_flight_sales;
-- Перезапустить DDS DAG (или только task load_dds_fact_flight_sales)

-- Проверка: airport_sk заполнены (через ods.routes), route_sk тоже (dim_routes заполнен на этой ветке)
SELECT
    COUNT(*) AS total,
    COUNT(departure_airport_sk) AS has_dep_sk,
    COUNT(arrival_airport_sk) AS has_arr_sk,
    COUNT(route_sk) AS has_route_sk
FROM dds.fact_flight_sales;
```

### После Этапа 4 (main branch)

1. `make up` → `make ddl-gp` → запустить STG+ODS+DDS DAG-и
2. Проверить: `sales_report` содержит данные с корректными аэропортами
3. Проверить: студенческие ODS/DDS-таблицы пусты (заглушки)
4. Fact DQ проходит (student SK = NULL, но DQ не блокирует)
5. Реализовать один студенческий dim → TRUNCATE fact → re-run → проверить SK

---

## Ключевые файлы

| Файл | Роль | Ветка |
|------|------|-------|
| `sql/dds/fact_flight_sales_load.sql` | Главное изменение: airport lookup через ods.routes | обе |
| `sql/dds/fact_flight_sales_dq.sql` | Student SK: EXCEPTION → NOTICE | main only |
| `sql/ods/routes_dq.sql` | Убрать RI airplane_code → ods.airplanes | main only |
| `airflow/dags/bookings_to_gp_ods.py` | DAG: убрать airplanes → routes | main only |
| `tests/test_ods_sql_contract.py` | SNAPSHOT_ENTITIES: убрать airplanes, seats | main only |
| `tests/test_dags_smoke.py` | ODS-барьер airplanes → routes | main only |
| `docs/assignment/analyst_spec.md` | Убрать STG-задания, убрать ods.routes | обе |
| `docs/design/assignment_design.md` | Обновить таблицы эталон/задание | обе |
| `docs/design/bookings_dds_design.md` | Обновить описание fact lookup | обе |
| `docs/design/bookings_ods_design.md` | Убрать зависимость routes от airplanes | main only |
| `docs/design/db_schema.md` | Пометить STG + ods.routes как эталон | обе |

> **STG не трогаем**: `sql/stg/routes_dq.sql`, `bookings_to_gp_stage.py`,
> STG smoke-тесты — без изменений. Весь STG эталонный, зависимости работают штатно.
