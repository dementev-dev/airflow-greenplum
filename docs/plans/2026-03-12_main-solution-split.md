# Plan: Подготовка main + ветка solution (Этап 4)

> Дата: 2026-03-12 | Версия: 4 (после третьего ревью Codex)
> Ветка: chore/bookings-etl → main → solution
> Спецификации кода: `docs/archive/2026-03-11_routes-to-reference.md` (п.2–5, п.8, п.10–11)

---

## 1. Мотивация

### Зачем раскладка по веткам

Сейчас весь код (эталон + студенческие задания) живёт в ветке `chore/bookings-etl`.
Студент должен получить **частично реализованный пайплайн**: эталонный вертикальный
срез работает «из коробки», а студенческие файлы — заглушки (`SELECT 1; -- TODO`).
Полная реализация доступна в ветке `solution` для самопроверки.

### Почему именно такой порядок

**Рассмотренные варианты:**

| # | Схема | Проблема |
|---|-------|----------|
| A | Создать solution от bookings-etl, потом на main — cherry-pick + заглушки | Грязные cherry-pick'и, main временно «не main» |
| B | Мерж в main, заглушки на main, solution = cherry-pick эталона обратно | main в какой-то момент содержит полный код, потом — заглушки; solution — через cherry-pick |
| **C** | **Мерж в main → общие правки → ветка solution (снимок) → main-only правки** | **Выбран** |

**Почему вариант C:**

- **main всегда остаётся main.** Нет момента, когда main «подменяется» другой веткой.
  Любые CI/CD, ссылки, клонирования — работают непрерывно.
- **Простая линейная история.** Мерж → общие правки → бранч → main-only коммиты.
  Никаких cherry-pick'ов.
- **solution — архивный снимок.** Не ожидается активная разработка. Студент сверяется
  с ним, а не мержит.
- **Общие правки до ветвления.** Документация, актуальная для обеих веток,
  обновляется *до* создания solution. Solution получает её автоматически.

### Стратегия синхронизации веток

После раскладки `main` (заглушки) и `solution` (полный код) расходятся.

**Главный принцип: solution — source of truth.**

Все изменения начинаются на solution (или feature-ветке от solution),
затем портируются в main. Направление потока: **solution → main**.

Файлы делятся на три категории:

| Категория | Примеры | Правило |
|-----------|---------|---------|
| **Общий код** | STG SQL, docker, Makefile, инфраструктура | Фиксим на solution, cherry-pick в main |
| **Общая документация** | README, TESTING, stack.md, naming_conventions | Фиксим на solution, cherry-pick в main |
| **Branch-specific** | заглушки load/dq, fact_flight_sales_dq (ослабленный), routes_dq (без RI), ODS DAG (без airplanes→routes), branch-specific формулировки в design docs | Фиксим на нужной ветке. Конфликтов нет — содержимое файлов разное |

**Исключение:** main-only правка (опечатка в заглушке, битая ссылка
в main-only документе) — правим прямо на main, solution не трогаем.

**Главное правило: никогда не мержим main → solution целиком.**

> Детальный протокол синхронизации — открытый вопрос, см. секцию 3.

---

## 2. Порядок выполнения

### Шаг 1. PR chore/bookings-etl → main

- Создать PR, ревью
- Мерж (squash или обычный — на усмотрение)
- После мержа: `git checkout main && git pull`

### Шаг 2. Общие правки на main (до ветвления solution)

> **Bootstrap-исключение:** стратегия синхронизации (секция 1) определяет
> solution как source of truth с потоком solution → main. Но при первичной
> раскладке solution ещё не существует — общие правки делаются на main,
> и solution наследует их при ветвлении (шаг 3). После создания solution
> действует штатный протокол.

Эти изменения отражают код, уже изменённый на bookings-etl
(fact_flight_sales_load.sql использует ods.routes). Документация должна
соответствовать коду на **обеих** ветках.

#### 2.1. Документация (общая для обеих веток)

- `docs/design/bookings_dds_design.md` — обновить описание fact lookup
  (airports через ods.routes, airplane через dim_routes point-in-time)
- `docs/bookings_to_gp_dds.md` — обновить описание DDS lookup
- `docs/bookings_to_gp_ods.md` — обновить описание ODS
- `docs/design/db_schema.md` — пометить STG + ods.routes как эталон

#### 2.2. TODO.md — переписать

Текущий текст Этапа 4 в TODO.md описывает устаревшую стратегию
(удалить routes из STG, убирать `\i` из ddl_gp.sql). Нужно обновить,
чтобы на solution осталась корректная история:

- Отметить этапы 2, 3 выполненными (✅)
- **Переписать** текст этапа 4 (не просто поставить галочку) — привести
  в соответствие с фактической стратегией (данный план)
- Отметить этап 4 выполненным после завершения

### Шаг 3. Создать ветку solution

```bash
git checkout main
git checkout -b solution
git push -u origin solution
```

Solution получает: полный эталонный код + актуальную общую документацию +
все design docs, plans, archive, TODO.md — полный контекст для мейнтейнера.

### Шаг 4. Main-only правки (заглушки, ослабление DQ)

Работаем на main (или на feature-ветке → PR в main).

#### 4.1. Код: fact_flight_sales_dq.sql — ослабить student SK

Спецификация: архивный план, п.2.

- `passenger_sk IS NULL` → `RAISE NOTICE` (было `RAISE EXCEPTION`)
- Разделить route-related блок: airport_sk (порог 1%, EXCEPTION) vs
  route_sk/airplane_sk (NOTICE only)
- Добавить учебный комментарий

#### 4.2. Код: routes_dq.sql — закомментировать RI airplane

Спецификация: архивный план, п.3.

- **Закомментировать** (не удалять) блок RI `airplane_code → ods.airplanes`
- Добавить комментарий:

```sql
-- Проверка RI airplane_code → ods.airplanes закомментирована,
-- т.к. таблица ods.airplanes реализуется студентом.
-- После реализации — раскомментируйте этот блок.
-- Полную версию см. в ветке solution.
```

#### 4.3. Код: ODS DAG — убрать зависимость airplanes → routes

Спецификация: архивный план, п.4.

- `[dq_ods_airports, dq_ods_airplanes] >> load_ods_routes`
  → `dq_ods_airports >> load_ods_routes`

#### 4.4. Заглушки: студенческие файлы

Спецификация: архивный план, п.5.

| Слой | Файлы (load + dq) | DDL |
|------|--------------------|-----|
| ODS | airplanes, seats | Оставить (таблица нужна) |
| DDS | dim_routes, dim_passengers, dim_airplanes | Оставить (нужен для LEFT JOIN) |
| DM | airport_traffic, route_performance, monthly_overview, passenger_loyalty | Оставить |

Формат заглушки load:
```sql
-- TODO: реализуйте загрузку (см. ТЗ в docs/assignment/analyst_spec.md)
-- Эталонную реализацию можно найти в ветке solution.
SELECT 1;
```

Формат заглушки dq:
```sql
-- TODO: реализуйте проверки качества данных
-- Эталонную реализацию можно найти в ветке solution.
SELECT 1;
```

#### 4.5. Тесты

Спецификация: архивный план, п.10.

- `test_dags_smoke.py`: убрать assert барьера `dq_ods_airplanes → dq_ods_routes`
- `test_ods_sql_contract.py`: `SNAPSHOT_ENTITIES = ("airports", "routes")`
  (убрать airplanes, seats — их load/dq теперь заглушки)

#### 4.6. Документация (main-specific)

- `docs/design/bookings_ods_design.md` — пометить airplanes/seats как студенческие,
  убрать зависимость routes от airplanes, скорректировать DQ-контракт
- `docs/design/bookings_dds_design.md` — обновить DQ-описание:
  student SK не блокируют (NOTICE), airport_sk — порог 1%
- `docs/bookings_to_gp_dds.md` — обновить: student SK будут NULL на main
- `docs/reference/qa-plan.md` — уточнить: ods.airplanes и ods.seats
  пусты by design на main

### Шаг 5. Очистка docs на main (после всех правок)

> Этот шаг выполняется **после** шагов 2–4, потому что:
> - solution уже создан (шаг 3) и сохранил все файлы
> - план и архивный справочник были доступны во время работы (шаг 4)

#### 5.1. Удалить с main (остаётся на solution)

| Файл/каталог | Почему не нужен студенту |
|--------------|------------------------|
| `docs/plans/` (весь каталог) | Планы разработки — внутренняя кухня |
| `docs/archive/` (весь каталог) | Архив планов — внутренняя кухня |
| `docs/design/PRD.md` | Продуктовые требования — внутренний документ |
| `docs/design/assignment_design.md` | Мета-дизайн задания (для авторов курса, не для студентов) |
| `docs/reference/bookings_generation_benchmark.md` | Бенчмарки генерации — отладочная информация |
| `docs/reference/bookings_tz.md` | Заметки о таймзонах — отладочная информация |
| `docs/reference/pxf_bookings.md` | PXF-конфигурация — внутренняя отладка |
| `docs/agent-dag-testing.md` | Инструкция для AI-агентов по тестированию |
| `docs/e2e-etl-test-protocol.md` | E2E-протокол — внутреннее тестирование |
| `TODO.md` | Таск-лист мейнтейнера |

#### 5.2. Что остаётся на main (полезно студенту)

| Файл | Зачем студенту |
|------|---------------|
| `README.md` | Установка, запуск, структура проекта |
| `TESTING.md` | Как проверять свою работу |
| `AGENTS.md`, `CLAUDE.md`, `GEMINI.md` | Если студент использует AI-помощников |
| `docs/README.md` | Навигация по документации |
| `docs/stack.md` | Стек технологий — контекст |
| `docs/dag_execution_order.md` | Порядок запуска DAG'ов |
| `docs/bookings_to_gp_stage.md` | Описание STG — эталонный код для изучения |
| `docs/bookings_to_gp_ods.md` | Описание ODS |
| `docs/bookings_to_gp_dds.md` | Описание DDS |
| `docs/bookings_to_gp_dm.md` | Описание DM |
| `docs/design/naming_conventions.md` | Нейминг полей — нужен для DDL/SQL |
| `docs/design/db_schema.md` | Схема БД — справочник |
| `docs/design/bookings_stg_design.md` | Дизайн STG — эталон для изучения |
| `docs/design/bookings_ods_design.md` | Дизайн ODS — нужен для задания |
| `docs/design/bookings_dds_design.md` | Дизайн DDS — нужен для задания |
| `docs/design/bookings_dm_design.md` | Дизайн DM — нужен для задания |
| `docs/assignment/` | Задание (analyst_spec.md) |
| `docs/reference/qa-plan.md` | QA-чеклист — полезен для самопроверки |
| `docs/reference/bookings_db_issues.md` | Известные проблемы — чтобы студент не тратил время на отладку |

#### 5.3. AGENTS.md — адаптировать для main

`AGENTS.md` содержит карту проекта и ссылки, которые побьются после cleanup.
Нужно обновить, а не просто добавить одну строку:

- **Карта проекта (секция 2):** убрать упоминания `docs/plans/`, `docs/archive/`.
  Добавить: «Полные дизайн-документы (PRD, assignment_design, планы) — в ветке solution»
- **Тестирование:** убрать ссылку на `docs/agent-dag-testing.md` (файл удалён)
- **Прочие ссылки:** проверить, что все пути в AGENTS.md ведут на существующие файлы

#### 5.4. Link audit — полная проверка ссылок

Проверить **все** ссылки во **всех** оставшихся на main markdown-файлах,
а не только ссылки на удалённые файлы. В документации уже есть битые ссылки
(напр. `docs/README.md` → `design/architecture_review.md`, файл в архиве).

**Метод:**

```bash
# 1. Найти все markdown-ссылки в оставшихся файлах
rg -o '\[.*?\]\([^)]+\.md[^)]*\)' docs/ README.md TESTING.md AGENTS.md

# 2. Проверить существование каждого целевого файла
# 3. Починить: удалить ссылку, обновить путь, или заменить на «см. ветку solution»
```

Известные проблемы (минимум):

- `docs/README.md` — ссылки на plans/, archive/, PRD, assignment_design, architecture_review
- `docs/assignment/README.md` — возможные ссылки на assignment_design
- `docs/design/bookings_stg_design.md` — ссылки на внутренние reference
- `docs/bookings_to_gp_stage.md` — ссылки на reference
- `docs/stack.md` — ссылки на reference
- `docs/design/db_schema.md` — ссылки на PRD, assignment_design
- `README.md` — ссылки на TODO.md, PRD
- `AGENTS.md` — покрыто шагом 5.3

### Шаг 6. Верификация main

```bash
make test                # smoke-тесты и контракты
make lint                # линтинг
```

При поднятом стенде:
1. `make up` → `make ddl-gp`
2. Запустить STG → ODS → DDS → DM DAG'и
3. Проверить: `sales_report` содержит данные с корректными аэропортами
4. Проверить: студенческие ODS/DDS-таблицы пусты
5. Fact DQ проходит (student SK = NULL, но DQ не блокирует)

### Шаг 7. Верификация solution

```bash
git checkout solution
make test
```

Убедиться, что полный пайплайн работает без изменений (это снимок
проверенного chore/bookings-etl + общие doc-правки).

---

## 3. Открытый вопрос: протокол синхронизации веток

> Не блокирует Этап 4. Доработать после раскладки, когда появится конкретный
> опыт (например, при первом PXF-задании).

### Предварительное решение: solution как source of truth

Все изменения (новые задания, баг-фиксы, доработки) начинаются на solution
или на feature-ветке от solution. Main — производная.

```
solution ← feat/new-task    (разработка + тесты)
                ↓ merge
solution                     (полный эталон, всегда рабочий)
                ↓ порт
main                         (cherry-pick общего + заглушки)
```

**Поток по умолчанию:** solution → main (одно направление).

**Исключение (редко):** main-only правка (опечатка в заглушке, битая ссылка
в main-only документе) — правим прямо на main, solution не трогаем.

### Что нужно доработать

- Как именно выглядит «порт в main»: cherry-pick, ручной перенос, чеклист?
- Что делать при конфликтах cherry-pick?
- Нужен ли реестр known-different файлов (заглушки vs реализации) для
  автоматической проверки синхронизации?
- Нужен ли периодический `git diff main..solution -- <shared files>` для
  обнаружения расхождений?

---

## 4. Ключевые файлы

Полная таблица с ролями и ветками: архивный план, секция «Ключевые файлы».

Дополнительно (docs cleanup, шаг 5.1):

| Действие | Файлы |
|----------|-------|
| Удалить с main | `docs/plans/*`, `docs/archive/*`, `docs/design/PRD.md`, `docs/design/assignment_design.md`, `docs/reference/bookings_generation_benchmark.md`, `docs/reference/bookings_tz.md`, `docs/reference/pxf_bookings.md`, `docs/agent-dag-testing.md`, `docs/e2e-etl-test-protocol.md`, `TODO.md` |
| Обновить на main | `AGENTS.md` (ссылка на solution), все .md с битыми ссылками (link audit) |
