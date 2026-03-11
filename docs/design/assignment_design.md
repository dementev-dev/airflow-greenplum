# Дизайн курсового задания

> Тактические решения по нарезке задания, порядку выполнения и самопроверке.
> Стратегию и контекст см. в [PRD.md](PRD.md).

---

## 1. Эталонный срез: витрина `sales_report`

Эталоном выбрана витрина `dm.sales_report` и вся её цепочка вниз по слоям.

**Почему `sales_report`:**
- Покрывает SCD1 (airports, tariffs), HWM-инкремент, fact load
- Богатая денормализация — хороший образец для подражания
- Средняя сложность — не пугает, но и не тривиальна

### Эталонные таблицы (даны студенту)

| Слой | Таблицы                                                         |
|------|-----------------------------------------------------------------|
| DM   | `sales_report`                                                  |
| DDS  | `fact_flight_sales`, `dim_airports` (SCD1), `dim_tariffs` (SCD1), `dim_calendar` |
| ODS  | `bookings`, `tickets`, `segments`, `flights`, `boarding_passes`, `airports` |
| STG  | `bookings`, `tickets`, `segments`, `flights`, `boarding_passes`, `airports` |

### Задание студенту

| Слой | Таблицы                                                           | Что нового для студента                          |
|------|-------------------------------------------------------------------|--------------------------------------------------|
| STG  | `airplanes`, `seats`, `routes`                                    | Практика по аналогии с эталоном                  |
| ODS  | `airplanes`, `seats`, `routes`                                    | Практика SCD1 UPSERT по аналогии                |
| DDS  | `dim_airplanes` (SCD1), `dim_passengers` (SCD1), `dim_routes` (SCD2) | **SCD2 — ключевой вызов курсовой**          |
| DM   | `airport_traffic`, `monthly_overview`, `route_performance`, `passenger_loyalty` | Разная сложность (от простой к сложной)  |

---

## 2. Рекомендуемый порядок выполнения

Студенту рекомендуется (но не обязательно) двигаться в таком порядке:

1. **STG** (airplanes, seats, routes) — разминка, по аналогии
2. **ODS** (airplanes, seats, routes) — закрепление UPSERT
3. **DDS** dim_airplanes, dim_passengers (SCD1) — новые измерения
4. **DDS** dim_routes (**SCD2**) — ключевой вызов
5. **DM** airport_traffic — простая витрина, похожа на sales_report
6. **DM** route_performance — TRUNCATE+INSERT, SCD2-агрегация по BK
7. **DM** monthly_overview — двухуровневая агрегация
8. **DM** passenger_loyalty — самая сложная, пересчёт истории

Порядок выстроен от простого к сложному. Каждый шаг опирается на опыт
предыдущего.

---

## 3. SCD2: подход «рецепт без готового SQL»

Реализация `dim_routes` (SCD2) — ключевой вызов курсовой. Студент делает это
самостоятельно, но ТЗ содержит пошаговую подсказку:

1. Алгоритм SCD2 текстом (без SQL):
   - Вычисли `hashdiff` по набору атрибутов (атрибуты перечислены в ТЗ)
   - Найди строки, у которых `hashdiff` изменился
   - Закрой старую версию (`valid_to = текущая_дата`)
   - Вставь новую версию (`valid_from = текущая_дата`, `valid_to = NULL`)
2. Формула hashdiff: `md5(concat_ws('|', field1, field2, ...))`
3. Ссылка на `naming_conventions.md` (поля `valid_from`, `valid_to`, `hashdiff`)
4. Напоминание: полуоткрытый интервал `[valid_from, valid_to)`
5. Если застрял — ветка `solution`

Самостоятельная реализация — ключ к запоминанию. SCD2 — обязательный вопрос
на собеседованиях DE, и студент должен уметь объяснить его на основе
собственного опыта.

---

## 4. Валидационный DAG (`bookings_validate`)

Отдельный DAG для самопроверки студента. Запускается вручную в Airflow UI
после реализации заданий. Таски сгруппированы по слоям — студент видит,
где именно проблема. Дополнительный бонус — практика чтения логов Airflow.

### Примерная структура тасков

```
bookings_validate
├── validate_stg
│   ├── check_stg_airplanes_exists      (таблица создана, >0 строк)
│   ├── check_stg_seats_exists
│   └── check_stg_routes_exists
├── validate_ods
│   ├── check_ods_airplanes_rowcount    (ODS >= STG по кол-ву уникальных BK)
│   ├── check_ods_seats_rowcount
│   ├── check_ods_routes_rowcount
│   └── check_ods_no_null_pks           (PK not null)
├── validate_dds
│   ├── check_dim_airplanes_exists
│   ├── check_dim_passengers_exists
│   ├── check_dim_routes_scd2           (valid_from/valid_to корректны)
│   └── check_dim_routes_no_gaps        (нет «дыр» в версиях SCD2)
└── validate_dm
    ├── check_airport_traffic_exists
    ├── check_monthly_overview_exists
    ├── check_route_performance_exists
    └── check_passenger_loyalty_exists
```

### Реализация

- `PostgresOperator` + SQL-скрипты в `sql/validate/`
- Каждый SQL-скрипт выполняет SELECT и бросает исключение (через
  `DO $$ ... RAISE EXCEPTION ... $$`), если проверка не пройдена
- Сообщения об ошибках — дружелюбные, с подсказкой что делать дальше

### Ключевые проверки

- Таблицы существуют и содержат данные
- PK не содержат NULL
- SCD2: `valid_to IS NULL` для текущих версий, нет перекрытий интервалов
- Кросс-слойная консистентность (row count ODS vs STG)
- DM-витрины содержат данные за загруженные дни

### Активная проверка SCD2 (`check_dim_routes_scd2`)

Справочник `bookings.routes` в демо-базе статичен — маршруты не меняются
между запусками генератора. Поэтому при обычном прогоне пайплайна студент
никогда не увидит, как SCD2 закрывает старую версию и создаёт новую.

Чтобы проверить корректность реализации, таск `check_dim_routes_scd2`
должен быть **активным** (не только читать, но и тестировать загрузку):

1. Сохранить текущее состояние `ods.routes` и `dds.dim_routes` (temp-таблицы).
2. Вставить в `ods.routes` тестовый маршрут с изменённым атрибутом
   (например, `scheduled_time` → `departure_time` сдвинут на 1 час).
3. Вызвать студенческий SQL загрузки `dim_routes` (`sql/dds/dim_routes_load.sql`).
4. Проверить результат:
   - Старая версия маршрута закрыта (`valid_to IS NOT NULL`).
   - Новая версия открыта (`valid_to IS NULL`, `hashdiff` отличается).
   - Нет «дыр» между `valid_to` старой и `valid_from` новой версии.
5. Откатить изменения: восстановить `ods.routes` и `dds.dim_routes`
   из сохранённых temp-таблиц.

Это единственный способ гарантировать, что SCD2 работает, без мутации
источника (что сломало бы генератор `continue()`).

---

## 5. Формат ТЗ от аналитика

Файл: `docs/assignment/analyst_spec.md` (или несколько файлов по слоям).

Для каждой таблицы-задания документ содержит:

- **Имя таблицы** и целевая схема (stg / ods / dds / dm)
- **Описание** — что хранит таблица, бизнес-смысл
- **Список полей** с типами и описанием
- **Маппинг источников** — откуда берётся каждое поле
- **Бизнес-правила и фильтры** (если есть)
- **Тип историзации** (SCD1 / SCD2 / snapshot / append)
- **Гранулярность** (одна строка = ?)
- **Distribution key** (подсказка или задание на выбор)

Формат — приближен к реальным ТЗ, которые студент встретит на работе.
