# Техническое задание: расширение DWH авиаперевозок

> **Роль:** Вы — Data Engineer. Это ТЗ подготовлено аналитиком на основе
> бизнес-потребностей. Ваша задача — реализовать описанные таблицы и загрузки,
> опираясь на эталонный срез (витрина `dm.sales_report` и вся её цепочка).
>
> **Эталон для изучения:**
> - STG: `bookings`, `tickets`, `flights`, `segments`, `boarding_passes`, `airports`
> - ODS: те же таблицы
> - DDS: `dim_airports`, `dim_tariffs`, `dim_calendar`, `fact_flight_sales`
> - DM: `sales_report`
>
> SQL-скрипты эталона лежат в `sql/stg/`, `sql/ods/`, `sql/dds/`, `sql/dm/`.

---

## Рекомендуемый порядок выполнения

1. **STG** — `airplanes`, `seats`, `routes` (разминка по аналогии)
2. **ODS** — `airplanes`, `seats`, `routes` (закрепление UPSERT / TRUNCATE+INSERT)
3. **DDS** — `dim_airplanes`, `dim_passengers` (SCD1 — новые измерения)
4. **DDS** — `dim_routes` (SCD2 — ключевой вызов курсовой)
5. **DM** — `airport_traffic` (простая витрина, похожа на `sales_report`)
6. **DM** — `route_performance` (Full Rebuild, работа с SCD2-измерением)
7. **DM** — `monthly_overview` (двухуровневая агрегация)
8. **DM** — `passenger_loyalty` (самая сложная, пересчёт истории)

Порядок выстроен от простого к сложному. Каждый шаг опирается на опыт
предыдущего. Вы можете двигаться в ином порядке, но убедитесь, что зависимости
слоёв соблюдены (STG → ODS → DDS → DM).

---

## Общие правила

- **Нейминг полей:** см. `docs/design/naming_conventions.md` — единый источник
  истины для служебных полей (`_load_id`, `_load_ts`, `created_at`, `updated_at`,
  `valid_from`, `valid_to`, `hashdiff`, суффиксы `_bk` / `_sk`).
- **SQL-файлы:** располагайте в `sql/{слой}/{объект}_{роль}.sql`
  (например, `sql/stg/airplanes_ddl.sql`, `sql/stg/airplanes_load.sql`).
- **DAG-интеграция:** используйте `PostgresOperator` + путь к SQL-файлу.
  Добавьте таски в существующие DAG-файлы соответствующего слоя.
- **Идемпотентность:** каждый скрипт загрузки должен быть безопасен
  при повторном запуске (не создавать дубликатов).
- **Шаблон `{{ run_id }}`:** используйте Jinja-шаблон Airflow для `_load_id`.

---

## Часть 1. STG-слой (Staging)

> **Цель:** Скопировать данные из источника (bookings-db) в Greenplum «как есть»,
> сохраняя все поля в текстовом виде. Преобразование типов — задача ODS.
>
> **Аналог для изучения:** `sql/stg/airports_ddl.sql`, `sql/stg/airports_load.sql`

### 1.1. stg.airplanes

**Описание:** Справочник моделей воздушных судов. Содержит технические
характеристики каждой модели: дальность полёта и крейсерскую скорость.

**Источник:** `bookings.airplanes_data` (через PXF)

| Поле | Тип | Описание | Маппинг из источника |
|------|-----|----------|----------------------|
| `airplane_code` | TEXT | Код модели самолёта (бизнес-ключ) | `airplanes_data.airplane_code::TEXT` |
| `model` | TEXT | Полное наименование модели (JSON в источнике) | `airplanes_data.model::TEXT` |
| `range` | TEXT | Дальность полёта, км | `airplanes_data.range::TEXT` |
| `speed` | TEXT | Крейсерская скорость, км/ч | `airplanes_data.speed::TEXT` |
| `event_ts` | TIMESTAMP | Момент загрузки (`now()`). У snapshot-справочников нет бизнес-события с точным временем, поэтому `event_ts` заполняется при загрузке | `now()` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки в STG | `now()` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча загрузки | `'{{ run_id }}'` |

**Тип историзации:** Нет (накопительный snapshot). Каждый запуск добавляет
новый батч по `_load_id`; прошлые батчи остаются в таблице. Не используйте TRUNCATE.

**Стратегия загрузки:** Полный снимок (Full Snapshot). Справочник маленький —
загружаем целиком каждый раз. Идемпотентность — через проверку `_load_id`.

**Distribution Key:** `airplane_code`

---

### 1.2. stg.seats

**Описание:** Карта посадочных мест для каждой модели самолёта.
Каждая строка — одно конкретное место в конкретной модели.

**Источник:** `bookings.seats` (через PXF)

| Поле | Тип | Описание | Маппинг из источника |
|------|-----|----------|----------------------|
| `airplane_code` | TEXT | Код модели самолёта (FK → airplanes) | `seats.airplane_code::TEXT` |
| `seat_no` | TEXT | Номер места (напр. «1A», «12C») | `seats.seat_no::TEXT` |
| `fare_conditions` | TEXT | Класс обслуживания (`Economy`, `Business`, `Comfort`) | `seats.fare_conditions::TEXT` |
| `event_ts` | TIMESTAMP | Момент загрузки (`now()`). Snapshot-справочник — бизнес-событие отсутствует | `now()` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки в STG | `now()` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча загрузки | `'{{ run_id }}'` |

**Тип историзации:** Нет (накопительный snapshot). Каждый запуск добавляет
новый батч по `_load_id`; прошлые батчи остаются в таблице. Не используйте TRUNCATE.

**Стратегия загрузки:** Полный снимок. Идемпотентность — через
`airplane_code` + `seat_no` + `_load_id`.

**Distribution Key:** `airplane_code`

---

### 1.3. stg.routes

**Описание:** Справочник авиамаршрутов. Маршрут — регулярный рейс между двумя
аэропортами на определённом типе самолёта с фиксированным расписанием.

**Источник:** `bookings.routes` (через PXF)

| Поле | Тип | Описание | Маппинг из источника |
|------|-----|----------|----------------------|
| `route_no` | TEXT | Номер маршрута (бизнес-ключ, часть 1) | `routes.route_no::TEXT` |
| `validity` | TEXT | Период действия маршрута (бизнес-ключ, часть 2) | `routes.validity::TEXT` |
| `departure_airport` | TEXT | Код аэропорта вылета (FK → airports) | `routes.departure_airport::TEXT` |
| `arrival_airport` | TEXT | Код аэропорта прилёта (FK → airports) | `routes.arrival_airport::TEXT` |
| `airplane_code` | TEXT | Код модели самолёта (FK → airplanes) | `routes.airplane_code::TEXT` |
| `days_of_week` | TEXT | Дни недели выполнения рейса | `routes.days_of_week::TEXT` |
| `scheduled_time` | TEXT | Время вылета по расписанию | `routes.scheduled_time::TEXT` |
| `duration` | TEXT | Плановая продолжительность полёта | `routes.duration::TEXT` |
| `event_ts` | TIMESTAMP | Момент загрузки (`now()`). Snapshot-справочник — бизнес-событие отсутствует | `now()` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки в STG | `now()` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча загрузки | `'{{ run_id }}'` |

**Тип историзации:** Нет (накопительный snapshot). Каждый запуск добавляет
новый батч по `_load_id`; прошлые батчи остаются в таблице. Не используйте TRUNCATE.

**Стратегия загрузки:** Полный снимок. Идемпотентность — по составному ключу
`route_no` + `validity` + `_load_id`.

**Distribution Key:** `route_no`

---

## Часть 2. ODS-слой (Operational Data Store)

> **Цель:** Привести данные из STG к целевым типам, очистить, дедуплицировать.
> Справочники (airplanes, seats, routes) загружаются стратегией TRUNCATE + INSERT
> из последнего согласованного батча STG.
>
> **Аналог для изучения:** `sql/ods/airports_ddl.sql`, `sql/ods/airports_load.sql`

### 2.1. ods.airplanes

**Описание:** Очищенный справочник моделей воздушных судов с правильными типами.

**Источник:** `stg.airplanes`

| Поле | Тип | Описание | Маппинг из STG |
|------|-----|----------|----------------|
| `airplane_code` | TEXT NOT NULL | Код модели (PK) | `airplane_code` |
| `model` | TEXT NOT NULL | Наименование модели | `model` (парсинг JSON: `model::JSON->>'ru'` — если JSON, иначе `model` как есть) |
| `range_km` | INTEGER | Дальность полёта, км | `range::INTEGER` |
| `speed_kmh` | INTEGER | Крейсерская скорость, км/ч | `speed::INTEGER` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (текущее состояние справочника, TRUNCATE + INSERT).

**Стратегия загрузки:** TRUNCATE + INSERT (полный снимок из STG).

**Бизнес-правила:**
- Привести `range` и `speed` из TEXT в INTEGER.
- Если `model` хранится в JSON-формате — извлечь русское название (`->>'ru'`).
  Изучите, как это сделано для `airports` в эталоне.

**Тип хранения Greenplum:** Append-Only Row.

**Distribution Key:** `airplane_code`

---

### 2.2. ods.seats

**Описание:** Карта посадочных мест с корректными типами.

**Источник:** `stg.seats`

| Поле | Тип | Описание | Маппинг из STG |
|------|-----|----------|----------------|
| `airplane_code` | TEXT NOT NULL | Код модели (PK, часть 1) | `airplane_code` |
| `seat_no` | TEXT NOT NULL | Номер места (PK, часть 2) | `seat_no` |
| `fare_conditions` | TEXT NOT NULL | Класс обслуживания | `fare_conditions` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (текущее состояние справочника, TRUNCATE + INSERT).

**Стратегия загрузки:** TRUNCATE + INSERT.

**Бизнес-правила:**
- Составной PK: `(airplane_code, seat_no)`.
- Значения `fare_conditions` ограничены: `Economy`, `Business`, `Comfort`.

**Тип хранения Greenplum:** Append-Only Row.

**Distribution Key:** `airplane_code`

---

### 2.3. ods.routes

**Описание:** Справочник маршрутов с правильными типами данных.

**Источник:** `stg.routes`

| Поле | Тип | Описание | Маппинг из STG |
|------|-----|----------|----------------|
| `route_no` | TEXT NOT NULL | Номер маршрута (PK, часть 1) | `route_no` |
| `validity` | TEXT NOT NULL | Период действия (PK, часть 2) | `validity` |
| `departure_airport` | TEXT NOT NULL | Код аэропорта вылета | `departure_airport` |
| `arrival_airport` | TEXT NOT NULL | Код аэропорта прилёта | `arrival_airport` |
| `airplane_code` | TEXT NOT NULL | Код модели самолёта | `airplane_code` |
| `days_of_week` | INTEGER[] | Дни недели (массив) | `days_of_week` — преобразовать TEXT в `INTEGER[]` |
| `departure_time` | TIME NOT NULL | Время вылета | `scheduled_time::TIME` |
| `duration` | INTERVAL NOT NULL | Длительность полёта | `duration::INTERVAL` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (текущее состояние справочника, TRUNCATE + INSERT).

**Стратегия загрузки:** TRUNCATE + INSERT.

**Бизнес-правила:**
- Составной PK: `(route_no, validity)`.
- Преобразование `days_of_week` из текста в массив целых чисел (`INTEGER[]`).
- Преобразование `scheduled_time` → `TIME`, `duration` → `INTERVAL`.

**Тип хранения Greenplum:** Append-Only Row.

**Distribution Key:** `(route_no, validity)`

---

## Часть 3. DDS-слой (Detailed Data Store) — Измерения

> **Цель:** Построить измерения звёздной схемы (Star Schema) с суррогатными
> ключами. SCD1-измерения обновляют атрибуты «на месте». SCD2-измерение
> хранит историю изменений через версионирование.
>
> **Аналог для SCD1:** `sql/dds/dim_airports_ddl.sql`, `sql/dds/dim_airports_load.sql`

### 3.1. dds.dim_airplanes (SCD1)

**Описание:** Измерение моделей самолётов. Содержит технические характеристики
и рассчитанное общее количество мест (обогащение из `ods.seats`).

**Источники:** `ods.airplanes` + `ods.seats`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `airplane_sk` | INTEGER NOT NULL | Суррогатный ключ | Генерация: `MAX(airplane_sk) + ROW_NUMBER()` |
| `airplane_bk` | TEXT NOT NULL | Бизнес-ключ (код модели) | `ods.airplanes.airplane_code` |
| `model` | TEXT NOT NULL | Название модели | `ods.airplanes.model` |
| `range_km` | INTEGER | Дальность полёта, км | `ods.airplanes.range_km` |
| `speed_kmh` | INTEGER | Скорость, км/ч | `ods.airplanes.speed_kmh` |
| `total_seats` | INTEGER | Общее кол-во мест | `COUNT(ods.seats.*) по airplane_code` |
| `created_at` | TIMESTAMP NOT NULL | Дата создания записи | `now()` при INSERT |
| `updated_at` | TIMESTAMP NOT NULL | Дата обновления | `now()` при UPDATE |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** SCD1 (обновление атрибутов без версионирования).

**Гранулярность:** Одна строка = одна модель самолёта.

**Стратегия загрузки:** UPSERT (UPDATE существующих + INSERT новых).
- UPDATE: если атрибуты (`model`, `range_km`, `speed_kmh`, `total_seats`)
  изменились (проверка через `IS DISTINCT FROM`).
- INSERT: если `airplane_bk` ещё не существует в `dim_airplanes`.

**Обогащение:** Поле `total_seats` рассчитывается как количество строк
в `ods.seats` для данного `airplane_code` (LEFT JOIN).

**Генерация суррогатного ключа:** `MAX(airplane_sk) + ROW_NUMBER()`.
Безопасно при `concurrency=1` в DAG.

**Distribution Key:** `airplane_sk`

---

### 3.2. dds.dim_passengers (SCD1)

**Описание:** Измерение пассажиров. Извлекается из таблицы билетов — каждый
уникальный `passenger_id` становится строкой измерения.

**Источник:** `ods.tickets`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `passenger_sk` | INTEGER NOT NULL | Суррогатный ключ | Генерация: `MAX(passenger_sk) + ROW_NUMBER()` |
| `passenger_id` | TEXT NOT NULL | Идентификатор пассажира (BK) | `ods.tickets.passenger_id` |
| `passenger_name` | TEXT NOT NULL | ФИО пассажира | `ods.tickets.passenger_name` |
| `created_at` | TIMESTAMP NOT NULL | Дата создания записи | `now()` при INSERT |
| `updated_at` | TIMESTAMP NOT NULL | Дата обновления | `now()` при UPDATE |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** SCD1.

**Гранулярность:** Одна строка = один уникальный пассажир.

**Стратегия загрузки:** UPSERT.
- Из `ods.tickets` один пассажир может встречаться в нескольких билетах.
  Необходима дедупликация: берём последнее (актуальное) имя.
  **Подсказка:** используйте `ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY event_ts DESC NULLS LAST, _load_ts DESC, ticket_no DESC)`
  для детерминированного выбора самой свежей записи.
- UPDATE: если `passenger_name` изменилось.
- INSERT: если `passenger_id` ещё не существует.

**Бизнес-правила:**
- `passenger_id` — бизнес-ключ. Один пассажир может иметь несколько билетов,
  но в измерении должна быть ровно одна строка.
- Имя обновляется, если изменилось в последнем билете (SCD1).

**Distribution Key:** `passenger_sk`

---

### 3.3. dds.dim_routes (SCD2)

> **Это ключевой вызов курсовой.** Реализация SCD Type 2 — обязательный навык
> для Data Engineer. Ниже — алгоритм текстом; SQL вы пишете самостоятельно.
> Если застряли — сверьтесь с веткой `solution`.

**Описание:** Измерение авиамаршрутов с полной историей изменений. Если у маршрута
меняется самолёт, аэропорт или расписание — создаётся новая версия, а старая
закрывается. Это позволяет видеть, какой маршрут действовал на момент конкретного
рейса.

**Источники:** `ods.routes` + `dds.dim_airports` + `dds.dim_airplanes`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `route_sk` | INTEGER NOT NULL | Суррогатный ключ | Генерация: `MAX(route_sk) + ROW_NUMBER()` |
| `route_bk` | TEXT NOT NULL | Бизнес-ключ маршрута | `ods.routes.route_no` |
| `departure_airport` | TEXT NOT NULL | Код аэропорта вылета | `ods.routes.departure_airport` |
| `arrival_airport` | TEXT NOT NULL | Код аэропорта прилёта | `ods.routes.arrival_airport` |
| `airplane_code` | TEXT NOT NULL | Код модели самолёта | `ods.routes.airplane_code` |
| `departure_city` | TEXT NOT NULL | Город вылета (денормализация) | `dds.dim_airports.city` по `departure_airport` |
| `arrival_city` | TEXT NOT NULL | Город прилёта (денормализация) | `dds.dim_airports.city` по `arrival_airport` |
| `airplane_model` | TEXT NOT NULL | Модель самолёта (денормализация) | `dds.dim_airplanes.model` |
| `total_seats` | INTEGER NOT NULL | Кол-во мест (денормализация) | `dds.dim_airplanes.total_seats` |
| `days_of_week` | TEXT | Дни недели | `ods.routes.days_of_week` (приведение к TEXT) |
| `departure_time` | TIME | Время вылета | `ods.routes.departure_time` |
| `duration` | INTERVAL | Длительность полёта | `ods.routes.duration` |
| `hashdiff` | TEXT NOT NULL | Хэш версионируемых атрибутов | См. формулу ниже |
| `valid_from` | DATE NOT NULL | Начало действия версии | Первая версия: `'1900-01-01'`; последующие: `CURRENT_DATE` |
| `valid_to` | DATE | Конец действия версии (NULL = текущая) | `NULL` для актуальных, `CURRENT_DATE` при закрытии |
| `created_at` | TIMESTAMP NOT NULL | Дата создания версии | `now()` при INSERT |
| `updated_at` | TIMESTAMP NOT NULL | Дата обновления | `now()` при UPDATE |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** SCD2 (версионирование с полуоткрытым интервалом).

**Гранулярность:** Одна строка = одна версия маршрута.

#### Формула hashdiff

```sql
md5(concat_ws('|',
    departure_airport,
    arrival_airport,
    airplane_code,
    days_of_week,
    departure_time,
    duration
))
```

Хэш считается по атрибутам, изменение которых означает «новую версию маршрута».
Денормализованные поля (`departure_city`, `arrival_city`, `airplane_model`,
`total_seats`) **не входят** в `hashdiff` — они обновляются отдельно (SCD1-refresh),
не создавая новую версию.

#### Алгоритм SCD2 (пошагово)

1. **Подготовка:** Рассчитайте `hashdiff` для каждого маршрута из `ods.routes`.
2. **Найдите изменения:** Сравните `hashdiff` текущих версий
   (`valid_to IS NULL`) с новыми значениями из ODS.
3. **Закройте устаревшие версии:** Для маршрутов, у которых `hashdiff` изменился,
   выполните UPDATE: `valid_to = CURRENT_DATE`, `updated_at = now()`.
4. **Закройте исчезнувшие маршруты:** Если маршрут был в DDS (`valid_to IS NULL`),
   но отсутствует в ODS — тоже закройте: `valid_to = CURRENT_DATE`.
5. **Вставьте новые версии:** INSERT строки с `valid_to = NULL`
   для изменённых и новых маршрутов. Значение `valid_from` зависит от того,
   встречался ли маршрут в DDS ранее:
   - **Первая версия** (маршрут ещё не было в DDS): `valid_from = '1900-01-01'`.
     Sentinel-дата покрывает всю историю полётов, чтобы point-in-time JOIN
     фактов на исторические рейсы находил корректный `route_sk`.
   - **Последующие версии** (маршрут уже был): `valid_from = CURRENT_DATE`.
6. **SCD1-обновление денормализованных полей:** Обновите `departure_city`,
   `arrival_city`, `airplane_model`, `total_seats` для ВСЕХ открытых версий
   (даже если `hashdiff` не менялся).

**Интервалы версий:** полуоткрытые `[valid_from, valid_to)`.
Текущая (актуальная) версия: `valid_to IS NULL`.

**Distribution Key:** `route_sk`

---

## Часть 4. DM-слой (Data Marts) — Витрины

> **Цель:** Построить аналитические витрины поверх DDS.
> Каждая витрина отвечает на конкретный бизнес-вопрос.
>
> **Аналог для изучения:** `sql/dm/sales_report_ddl.sql`, `sql/dm/sales_report_load.sql`

### 4.1. dm.airport_traffic

**Бизнес-вопрос:** «Какой пассажиропоток и выручка у каждого аэропорта по дням?»

**Описание:** Ежедневная статистика по каждому аэропорту: сколько рейсов
вылетело/прилетело, сколько пассажиров, какая выручка. Аэропорт выступает
в двойной роли — и как точка вылета, и как точка прилёта.

**Источники:** `dds.fact_flight_sales` + `dds.dim_airports` + `dds.dim_calendar`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `traffic_date` | DATE NOT NULL | Дата (зерно, часть 1) | `dim_calendar.date_actual` |
| `airport_sk` | INTEGER NOT NULL | Суррогатный ключ аэропорта (зерно, часть 2) | `fact.departure_airport_sk` / `fact.arrival_airport_sk` (UNION ALL) |
| `airport_bk` | TEXT NOT NULL | Код аэропорта (денормализация) | `dim_airports.airport_bk` |
| `city` | TEXT NOT NULL | Город (денормализация) | `dim_airports.city` |
| `departures_flights` | INTEGER NOT NULL | Кол-во рейсов на вылет | `COUNT(DISTINCT flight_id)` роль «departure» |
| `departures_passengers` | INTEGER NOT NULL | Пассажиры на вылет | `SUM(is_boarded)` роль «departure» |
| `departures_revenue` | NUMERIC(15,2) NOT NULL | Выручка по вылетам | `SUM(price)` роль «departure» |
| `arrivals_flights` | INTEGER NOT NULL | Кол-во рейсов на прилёт | `COUNT(DISTINCT flight_id)` роль «arrival» |
| `arrivals_passengers` | INTEGER NOT NULL | Пассажиры на прилёт | `SUM(is_boarded)` роль «arrival» |
| `arrivals_revenue` | NUMERIC(15,2) NOT NULL | Выручка по прилётам | `SUM(price)` роль «arrival» |
| `total_passengers` | INTEGER NOT NULL | Общий пассажиропоток (вылет + прилёт) | `SUM(is_boarded)` обе роли |
| `created_at` | TIMESTAMP NOT NULL | Дата создания записи | `now()` при INSERT |
| `updated_at` | TIMESTAMP NOT NULL | Дата обновления | `now()` при UPDATE |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (UPSERT — текущее состояние метрик за день).

**Гранулярность:** Одна строка = один аэропорт за один день.

**Стратегия загрузки:** Инкрементальный UPSERT с HWM по `fact_flight_sales._load_ts`.

**Ключевой приём — Unpivot (UNION ALL):**
Каждый факт продажи порождает два «события»: вылет (departure) и прилёт (arrival).
Используйте UNION ALL для разворота факта в два ряда — по `departure_airport_sk`
и `arrival_airport_sk`. Затем сгруппируйте по `(traffic_date, airport_sk)`.

**Метрики:**
- `departures_flights` — `COUNT(DISTINCT flight_id)` для роли «вылет»
- `departures_passengers` — `SUM(is_boarded)` для роли «вылет»
- `departures_revenue` — `SUM(price)` для роли «вылет»
- Аналогично для прилётов
- `total_passengers` — сумма всех `is_boarded` (обе роли)

**Важно:** Выручка специально разделена на `departures_revenue` и `arrivals_revenue`.
Суммировать их нельзя — это приведёт к двойному счёту (один билет учитывается и
в аэропорту вылета, и в аэропорту прилёта).

**Тип хранения Greenplum:** Heap (для UPSERT).

**Distribution Key:** `airport_sk`

---

### 4.2. dm.route_performance

**Бизнес-вопрос:** «Какие маршруты самые эффективные? Где высокий load factor,
а где теряем пассажиров?»

**Описание:** Сводная статистика эффективности каждого маршрута за всё время.
Агрегация идёт по бизнес-ключу маршрута (`route_bk`), чтобы собрать данные
со всех исторических версий (SCD2).

**Источники:** `dds.fact_flight_sales` + `dds.dim_routes` + `dds.dim_calendar`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `route_bk` | TEXT NOT NULL | Бизнес-ключ маршрута (зерно) | `dim_routes.route_bk` (GROUP BY) |
| `route_sk` | INTEGER NOT NULL | SK актуальной версии маршрута | `dim_routes.route_sk` WHERE `valid_to IS NULL` |
| `departure_airport_bk` | TEXT NOT NULL | Код аэропорта вылета (денормализация) | `dim_routes.departure_airport` (актуальная версия) |
| `departure_city` | TEXT NOT NULL | Город вылета (денормализация) | `dim_routes.departure_city` (актуальная версия) |
| `arrival_airport_bk` | TEXT NOT NULL | Код аэропорта прилёта (денормализация) | `dim_routes.arrival_airport` (актуальная версия) |
| `arrival_city` | TEXT NOT NULL | Город прилёта (денормализация) | `dim_routes.arrival_city` (актуальная версия) |
| `airplane_bk` | TEXT NOT NULL | Код модели самолёта (денормализация) | `dim_routes.airplane_code` (актуальная версия) |
| `airplane_model` | TEXT NOT NULL | Модель самолёта (денормализация) | `dim_routes.airplane_model` (актуальная версия) |
| `total_seats` | INTEGER NOT NULL | Кол-во мест в самолёте (денормализация) | `dim_routes.total_seats` (актуальная версия) |
| `total_flights` | INTEGER NOT NULL | Всего рейсов | `COUNT(DISTINCT fact.flight_id)` |
| `total_tickets` | INTEGER NOT NULL | Всего проданных билетов | `COUNT(*)` |
| `total_boarded` | INTEGER NOT NULL | Всего посадок | `SUM(CASE WHEN is_boarded THEN 1 ELSE 0 END)` |
| `total_revenue` | NUMERIC(15,2) NOT NULL | Суммарная выручка | `SUM(fact.price)` |
| `avg_ticket_price` | NUMERIC(10,2) | Средняя цена билета | `total_revenue / NULLIF(total_tickets, 0)` |
| `avg_boarding_rate` | NUMERIC(5,4) NOT NULL | Средняя доля посадок | `AVG(CASE WHEN is_boarded THEN 1 ELSE 0 END)` |
| `avg_load_factor` | NUMERIC(5,4) | Средняя заполняемость кресел | `total_boarded / NULLIF(total_flights * total_seats, 0)` |
| `first_flight_date` | DATE | Дата первого рейса | `MIN(dim_calendar.date_actual)` |
| `last_flight_date` | DATE | Дата последнего рейса | `MAX(dim_calendar.date_actual)` |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (Full Rebuild — полная перезагрузка каждый запуск).

**Гранулярность:** Одна строка = один маршрут (по `route_bk`).

**Стратегия загрузки:** Full Rebuild (TRUNCATE + INSERT).
Витрина небольшая (~1000 строк) — проще пересоздать, чем вычислять дельту.

**Обработка SCD2:** Факты связаны с разными версиями маршрута (`route_sk`).
Агрегируйте метрики по `route_bk` (бизнес-ключу), чтобы собрать статистику
со ВСЕХ версий. Денормализованные атрибуты берите из ТЕКУЩЕЙ версии
(`valid_to IS NULL`).

**Метрики:**
- `avg_ticket_price` — `total_revenue / total_tickets` (защита от деления на 0
  через `NULLIF`)
- `avg_boarding_rate` — `AVG(CASE WHEN is_boarded THEN 1 ELSE 0 END)`
- `avg_load_factor` — `total_boarded / (total_flights * total_seats)`

**Тип хранения Greenplum:** AO Column Store (zstd). Идеален для аналитики:
отличное сжатие, чтение только нужных колонок. AO не поддерживает UPDATE —
поэтому используем Full Rebuild.

**Distribution Key:** `route_bk`

**Служебные поля:** `created_at` / `updated_at` здесь не нужны — при Full Rebuild
все строки пересоздаются. Достаточно `_load_ts`.

---

### 4.3. dm.monthly_overview

**Бизнес-вопрос:** «Какова помесячная динамика: рейсы, выручка, load factor
в разрезе типов самолётов?»

**Описание:** Помесячная сводка с точным расчётом средней заполняемости (load factor)
через двухуровневую агрегацию. Разрез — по типу самолёта.

**Источники:** `dds.fact_flight_sales` + `dds.dim_calendar` + `dds.dim_airplanes`
+ `dds.dim_routes`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `year_actual` | INTEGER NOT NULL | Год (зерно, часть 1) | `dim_calendar.year_actual` |
| `month_actual` | INTEGER NOT NULL | Месяц (зерно, часть 2) | `dim_calendar.month_actual` |
| `airplane_sk` | INTEGER NOT NULL | SK типа самолёта (зерно, часть 3) | `fact.airplane_sk` |
| `airplane_bk` | TEXT NOT NULL | Код модели (денормализация) | `dim_airplanes.airplane_bk` |
| `airplane_model` | TEXT NOT NULL | Название модели (денормализация) | `dim_airplanes.model` |
| `total_seats` | INTEGER NOT NULL | Кол-во мест (денормализация) | `dim_airplanes.total_seats` |
| `total_flights` | INTEGER NOT NULL | Кол-во уникальных рейсов | `COUNT(DISTINCT fact.flight_id)` |
| `total_tickets` | INTEGER NOT NULL | Кол-во проданных билетов | `SUM(tickets_sold_per_flight)` (уровень 2) |
| `total_boarded` | INTEGER NOT NULL | Кол-во посадок | `SUM(boarded_per_flight)` (уровень 2) |
| `total_revenue` | NUMERIC(15,2) NOT NULL | Суммарная выручка | `SUM(revenue_per_flight)` (уровень 2) |
| `avg_ticket_price` | NUMERIC(10,2) | Средняя цена билета | `total_revenue / NULLIF(total_tickets, 0)` |
| `avg_load_factor` | NUMERIC(5,4) | Средняя заполняемость кресел | `AVG(flight_load_factor)` (уровень 2) |
| `unique_routes` | INTEGER NOT NULL | Кол-во уникальных маршрутов | `COUNT(DISTINCT dim_routes.route_bk)` |
| `unique_passengers` | INTEGER NOT NULL | Кол-во уникальных пассажиров | `COUNT(DISTINCT fact.passenger_sk)` |
| `created_at` | TIMESTAMP NOT NULL | Дата создания записи | `now()` при INSERT |
| `updated_at` | TIMESTAMP NOT NULL | Дата обновления | `now()` при UPDATE |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (UPSERT — текущее состояние метрик за месяц).

**Гранулярность:** Одна строка = один месяц + один тип самолёта.

**Стратегия загрузки:** Инкрементальный UPSERT с HWM по `fact_flight_sales._load_ts`.
Пересчитываются только затронутые месяцы.

**Ключевой приём — Двухуровневая агрегация:**

Чтобы честно посчитать среднюю заполняемость (`avg_load_factor`), нельзя просто
поделить `SUM(boarded)` на `SUM(seats)` — это даёт ошибку (парадокс Симпсона).
Правильный путь:
1. **Уровень 1 (рейс):** Для каждого `flight_id` посчитайте `load_factor = boarded / total_seats`.
2. **Уровень 2 (месяц):** Возьмите `AVG(load_factor)` по всем рейсам месяца.

**Подсчёт уникальных маршрутов:** Используйте `COUNT(DISTINCT route_bk)` из
`dim_routes` (т.к. `dim_routes` — SCD2, у одного маршрута может быть несколько
`route_sk`).

**Антипаттерн (Distribution Key):**
Распределение по `(year_actual, month_actual)` — это ошибка. В MPP-системах
распределение по дате ведёт к Data Skew (весь месяц на одном сегменте).
Используйте `airplane_sk`.

**Тип хранения Greenplum:** Heap (для UPSERT).

**Distribution Key:** `airplane_sk`

---

### 4.4. dm.passenger_loyalty

**Бизнес-вопрос:** «Кто наши самые лояльные пассажиры? Сколько они летают,
тратят, какой класс предпочитают?»

**Описание:** Профиль лояльности каждого пассажира: накопительные метрики
за всю историю перелётов. Самая сложная витрина — требует пересчёта
полной истории для затронутых пассажиров.

**Источники:** `dds.fact_flight_sales` + `dds.dim_passengers` + `dds.dim_tariffs`
+ `dds.dim_routes` + `dds.dim_calendar`

| Поле | Тип | Описание | Маппинг |
|------|-----|----------|---------|
| `passenger_sk` | INTEGER NOT NULL | SK пассажира (зерно) | `fact.passenger_sk` |
| `passenger_bk` | TEXT NOT NULL | Идентификатор пассажира (денормализация) | `dim_passengers.passenger_id` |
| `passenger_name` | TEXT NOT NULL | ФИО (денормализация) | `dim_passengers.passenger_name` |
| `total_bookings` | INTEGER NOT NULL | Кол-во бронирований (`book_ref`) | `COUNT(DISTINCT fact.book_ref)` |
| `total_flights` | INTEGER NOT NULL | Кол-во перелётов | `COUNT(*)` |
| `total_boarded` | INTEGER NOT NULL | Кол-во успешных посадок | `SUM(CASE WHEN is_boarded THEN 1 ELSE 0 END)` |
| `total_spent` | NUMERIC(15,2) NOT NULL | Общие траты | `SUM(fact.price)` |
| `avg_ticket_price` | NUMERIC(10,2) | Средняя цена билета | `total_spent / NULLIF(total_flights, 0)` |
| `favorite_fare_conditions` | TEXT | Самый частый класс обслуживания | Мода по `dim_tariffs.fare_conditions` |
| `unique_routes` | INTEGER NOT NULL | Кол-во уникальных маршрутов | `COUNT(DISTINCT dim_routes.route_bk)` |
| `first_flight_date` | DATE | Дата первого перелёта | `MIN(dim_calendar.date_actual)` |
| `last_flight_date` | DATE | Дата последнего перелёта | `MAX(dim_calendar.date_actual)` |
| `days_as_customer` | INTEGER | Стаж клиента (дней между первым и последним) | `last_flight_date - first_flight_date` |
| `created_at` | TIMESTAMP NOT NULL | Дата создания записи | `now()` при INSERT |
| `updated_at` | TIMESTAMP NOT NULL | Дата обновления | `now()` при UPDATE |
| `_load_id` | TEXT NOT NULL | Идентификатор батча | `'{{ run_id }}'` |
| `_load_ts` | TIMESTAMP NOT NULL | Момент загрузки | `now()` |

**Тип историзации:** Нет (UPSERT — текущий профиль пассажира).

**Гранулярность:** Одна строка = один пассажир.

**Стратегия загрузки:** Инкрементальный UPSERT по «затронутым ключам».

**Ключевой приём — Метод затронутых ключей:**

Это не обычный HWM по датам. Алгоритм:
1. Найдите `passenger_sk`, чьи факты изменились (HWM по `fact._load_ts`).
2. Для этих пассажиров **пересчитайте ВСЮ историю** — все их перелёты от начала.
3. UPSERT результаты.

Почему? Потому что метрики накопительные (`total_spent`, `first_flight_date`).
Нельзя просто добавить дельту — нужен полный пересчёт для корректности.

**Метрики:**
- `total_bookings` — `COUNT(DISTINCT book_ref)` по всем перелётам пассажира
- `favorite_fare_conditions` — мода (самое частое значение).
  **Подсказка:** используйте `DISTINCT ON` с `ORDER BY COUNT(*) DESC`.
- `unique_routes` — `COUNT(DISTINCT route_bk)` (не `route_sk`! т.к. `dim_routes` — SCD2)
- `days_as_customer` — `last_flight_date - first_flight_date`
- `avg_ticket_price` — `total_spent / total_flights`

**Фильтрация NULL:** В `fact_flight_sales` поле `passenger_sk` может быть NULL
(защитная фильтрация от неконсистентных фактов). Исключите такие строки:
`WHERE passenger_sk IS NOT NULL`.

**Тип хранения Greenplum:** Heap (для UPSERT).

**Distribution Key:** `passenger_sk`

---

## Валидация

После реализации каждого слоя запустите валидационный DAG `bookings_validate`
в Airflow UI. Он проверит:

- Таблицы существуют и содержат данные
- PK не содержат NULL
- SCD2: корректность `valid_from`/`valid_to`, отсутствие «дыр» в версиях
- Кросс-слойная консистентность (ODS vs STG по кол-ву записей)
- DM-витрины содержат данные за загруженные дни

Сообщения об ошибках укажут, что именно не так и что делать дальше.

---

## Приложение: ER-диаграмма (источник)

```
bookings.airplanes_data          bookings.seats
  airplane_code (PK)               airplane_code (FK) ──┐
  model (JSON)                     seat_no             │
  range                            fare_conditions     │
  speed                                                │
       │                                               │
       └──────────────────────────────────────────────┘
       │
bookings.routes
  route_no (PK, part 1)
  validity (PK, part 2)
  departure_airport (FK → airports_data)
  arrival_airport   (FK → airports_data)
  airplane_code     (FK → airplanes_data)
  days_of_week
  scheduled_time
  duration

bookings.flights
  flight_id (PK)
  route_no  (FK → routes)
  status
  scheduled_departure / scheduled_arrival
  actual_departure    / actual_arrival

bookings.bookings ─── bookings.tickets ─── bookings.segments
  book_ref (PK)         ticket_no (PK)       ticket_no (FK)
  book_date             book_ref (FK)        flight_id (FK)
  total_amount          passenger_id         fare_conditions
                        passenger_name       price
                        outbound

bookings.boarding_passes
  ticket_no (FK)
  flight_id (FK)
  seat_no
  boarding_no
  boarding_time
```
