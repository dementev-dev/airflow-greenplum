# План: Денормализация dim_routes + упрощение route_performance

> **Статус:** реализовано.

## Контекст

`dds.dim_routes` хранит только FK-ссылки (`departure_airport`, `arrival_airport`, `airplane_code`) без атрибутов (город, модель самолёта, кол-во мест). Из-за этого витрина `dm.route_performance` вынуждена делать 4-JOIN цепочку вместо одного JOIN к измерению. Это противоречит принципу Кимбалла (измерение должно быть «самодостаточным») и усложняет учебный материал.

**Цель:** добавить в dim_routes 4 денормализованных колонки, упростить route_performance до 1 JOIN, показать студентам правильную Star Schema.

---

## Файлы для изменения

| # | Файл | Суть |
|---|---|---|
| 1 | `sql/dds/dim_routes_ddl.sql` | +4 колонки через ALTER TABLE |
| 2 | `sql/dds/dim_routes_load.sql` | JOIN к airports/airplanes при INSERT + refresh-шаг |
| 3 | `sql/dds/dim_routes_dq.sql` | NOT NULL проверка для новых колонок (current-срез) |
| 4 | `airflow/dags/bookings_to_gp_dds.py` | Зависимость: airports+airplanes DQ → routes load |
| 5 | `sql/dm/route_performance_load.sql` | Упрощение 4-JOIN → 1-JOIN |
| 6 | `tests/test_dags_smoke.py` | Новые assert для зависимости routes от airports/airplanes |
| 7 | `docs/internal/db_schema.md` | Обновить схему dim_routes |

---

## Шаг 1. DDL — `sql/dds/dim_routes_ddl.sql`

После существующего `COMMENT ON TABLE` добавить учебный комментарий и 4 ALTER TABLE:

```sql
-- Денормализация атрибутов из SCD1-измерений (dim_airports, dim_airplanes).
--
-- Учебный комментарий (Kimball Star Schema):
-- Измерение должно быть «самодостаточным»: один JOIN к dim_routes —
-- и аналитик видит маршрут, города, модель самолёта и кол-во мест.
--
-- Эти колонки НЕ участвуют в hashdiff. Версия SCD2 фиксирует изменения
-- атрибутов маршрута (аэропорт, самолёт, расписание). Если изменится
-- название города — обновим отдельным refresh-шагом, не создавая новую версию.
ALTER TABLE dds.dim_routes ADD COLUMN IF NOT EXISTS departure_city  TEXT;
ALTER TABLE dds.dim_routes ADD COLUMN IF NOT EXISTS arrival_city    TEXT;
ALTER TABLE dds.dim_routes ADD COLUMN IF NOT EXISTS airplane_model  TEXT;
ALTER TABLE dds.dim_routes ADD COLUMN IF NOT EXISTS total_seats     INTEGER;
```

Колонки nullable — `ALTER TABLE ADD COLUMN ... NOT NULL` без DEFAULT упадёт на существующих строках. DQ проверит NOT NULL для current-среза.

---

## Шаг 2. Load — `sql/dds/dim_routes_load.sql`

Структура: 3 фазы вместо 2.

**Фаза 1 (без изменений):** temp table + hashdiff из ods.routes, UPDATE закрытие версий. Hashdiff **не включает** денормализованные атрибуты.

**Фаза 2 (изменён INSERT):** при вставке новой версии — LEFT JOIN к dim_airports (×2) и dim_airplanes для заполнения departure_city, arrival_city, airplane_model, total_seats.

Конкретно: в INSERT (Statement 2, строки 59-107) добавить:
- В список колонок: `departure_city, arrival_city, airplane_model, total_seats`
- В SELECT: `dep.city, arr.city, air.model, air.total_seats`
- LEFT JOIN к dim_airports и dim_airplanes (LEFT — чтобы не терять маршруты при отсутствии аэропорта; DQ поймает)

**Фаза 3 (новая):** refresh денормализованных атрибутов для всех current-версий:

```sql
-- Фаза 3: Обновление денормализованных атрибутов (refresh).
-- Нужна для SCD1-изменений в dim_airports/dim_airplanes (напр. переименование города).
-- Обновляем ТОЛЬКО current-версии (valid_to IS NULL).
UPDATE dds.dim_routes AS d
SET departure_city = dep.city,
    arrival_city   = arr.city,
    airplane_model = air.model,
    total_seats    = air.total_seats,
    updated_at     = now(),
    _load_id       = '{{ run_id }}',
    _load_ts       = now()
FROM dds.dim_airports AS dep,
     dds.dim_airports AS arr,
     dds.dim_airplanes AS air
WHERE d.valid_to IS NULL
    AND dep.airport_bk = d.departure_airport
    AND arr.airport_bk = d.arrival_airport
    AND air.airplane_bk = d.airplane_code
    AND (
        d.departure_city IS DISTINCT FROM dep.city
        OR d.arrival_city IS DISTINCT FROM arr.city
        OR d.airplane_model IS DISTINCT FROM air.model
        OR d.total_seats IS DISTINCT FROM air.total_seats
    );
```

Этот же шаг при первом запуске заполнит колонки для существующих данных (backfill).

---

## Шаг 3. DQ — `sql/dds/dim_routes_dq.sql`

Перед финальным `RAISE NOTICE` добавить проверку:

```sql
-- Денормализованные поля не пустые в текущих (актуальных) версиях.
-- Исторические версии могли быть загружены ДО добавления колонок — пропускаем.
SELECT COUNT(*) INTO v_null_count
FROM dds.dim_routes
WHERE valid_to IS NULL
    AND (departure_city IS NULL OR arrival_city IS NULL
         OR airplane_model IS NULL OR total_seats IS NULL);

IF v_null_count <> 0 THEN
    RAISE EXCEPTION
        'DQ FAILED: в current-срезе dim_routes найдены NULL денормализованные поля: %',
        v_null_count;
END IF;
```

---

## Шаг 4. DAG — `airflow/dags/bookings_to_gp_dds.py`

Убрать `load_dds_dim_routes` из параллельного fan-out, добавить зависимость от DQ airports и airplanes:

```python
# Было:
dq_dds_dim_calendar >> [
    load_dds_dim_airports,
    load_dds_dim_airplanes,
    load_dds_dim_tariffs,
    load_dds_dim_passengers,
    load_dds_dim_routes,       # параллельно
]

# Стало:
dq_dds_dim_calendar >> [
    load_dds_dim_airports,
    load_dds_dim_airplanes,
    load_dds_dim_tariffs,
    load_dds_dim_passengers,
]

# dim_routes зависит от airports и airplanes (денормализация).
[dq_dds_dim_airports, dq_dds_dim_airplanes] >> load_dds_dim_routes
```

Транзитивная зависимость от calendar сохраняется через airports/airplanes.

---

## Шаг 5. Витрина — `sql/dm/route_performance_load.sql`

Упростить Шаг 3. Было 4 JOIN → станет 1 JOIN:

```sql
FROM tmp_route_metrics m
JOIN dds.dim_routes r_curr
    ON m.route_bk = r_curr.route_bk AND r_curr.valid_to IS NULL
```

SELECT использует `r_curr.departure_airport AS departure_airport_bk`, `r_curr.departure_city`, `r_curr.airplane_code AS airplane_bk`, `r_curr.airplane_model`, `r_curr.total_seats`. Добавить учебный комментарий: «Благодаря денормализации dim_routes — один JOIN вместо четырёх.»

---

## Шаг 6. Тесты — `tests/test_dags_smoke.py`

Добавить в `test_bookings_to_gp_dds_dag_structure`:

```python
# dim_routes зависит от airports и airplanes (денормализация).
_assert_reachable(dag, "dq_dds_dim_airports", "load_dds_dim_routes")
_assert_reachable(dag, "dq_dds_dim_airplanes", "load_dds_dim_routes")
```

---

## Шаг 7. Документация — `docs/internal/db_schema.md`

Обновить описание dim_routes: добавить 4 новые колонки, пометить «денормализовано из dim_airports/dim_airplanes». Обновить граф зависимостей DAG.

---

## Что НЕ меняется

- **fact_flight_sales_load.sql** — факт резолвит SK через свои JOIN-ы. Без изменений.
- **passenger_loyalty, monthly_overview** — используют только `route_bk` из dim_routes.
- **airport_traffic** — вообще не джойнит dim_routes.
- **hashdiff** — остаётся прежним (только атрибуты из ods.routes).

---

## Верификация

1. `make ddl-gp` — накатить DDL (ALTER TABLE добавит колонки)
2. Запустить DAG `bookings_to_gp_dds` — проверить:
   - airports/airplanes грузятся ДО routes (Graph view)
   - Фаза 3 (refresh) заполняет departure_city, arrival_city, airplane_model, total_seats
   - DQ проходит без ошибок
3. Запустить DAG `bookings_to_gp_dm` — проверить:
   - route_performance загружается с 1 JOIN
   - Витрина содержит корректные города и модели самолётов
4. `make test` — smoke-тесты DAG проходят (включая новые assert'ы)
5. SQL-проверка:
   ```sql
   SELECT route_bk, departure_city, arrival_city, airplane_model, total_seats
   FROM dds.dim_routes WHERE valid_to IS NULL LIMIT 5;
   ```
