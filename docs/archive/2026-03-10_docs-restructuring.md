# План ревизии документации

> Статус: **ЧЕРНОВИК v3** | Дата: 2026-03-10
> Контекст: перед Этапом 2 (подготовка main) нужно навести порядок в docs/

---

## Проблемы

- `docs/internal/` — свалка: дизайн-документы, планы, ревью, баг-трекеры, стандарты
- 3 архивных плана лежат рядом с живыми документами (неотличимы)
- 3 осиротевших документа (никто не ссылается)
- `educational-tasks.md` в корне — устарел (раздел 2.2 говорит «ODS/DDS/DM будут позже»)
- 5 документов содержат устаревшие фрагменты
- `docs/README.md` не знает про несколько живых документов

---

## Принципы

- **Архив замораживается.** Файлы в `docs/archive/` не правим — ссылки внутри них
  могут быть битыми, это ожидаемо. Они сохраняются как исторические артефакты.
- **Активные планы** живут в `docs/plans/`, после выполнения переезжают в `docs/archive/`.
- **Студенческий entry point** не должен исчезать: пока `analyst_spec.md` (Этап 3)
  не создан, в `docs/assignment/` будет заглушка `README.md` со ссылкой на эталонный
  срез для самостоятельного изучения.

---

## Фаза 1. Структура каталогов

Создать новые каталоги:

```
docs/design/       — дизайн-документы, стандарты, архитектура
docs/reference/    — техническая справка, баг-трекеры, бенчмарки
docs/plans/        — активные планы работ
docs/archive/      — выполненные планы, закрытые ревью
docs/assignment/   — заглушка README.md (подготовка для Этапа 3)
```

---

## Фаза 2. Перемещение файлов

### В `docs/archive/` (4 файла)

| Откуда | Файл | Причина |
|--------|-------|---------|
| корень | `educational-tasks.md` | Устарел, заменён `assignment_design.md` |
| `docs/internal/` | `stg_naming_unification_plan.md` | Выполнен |
| `docs/internal/` | `dim_routes_denormalization_plan.md` | Выполнен |
| `docs/internal/` | `bookings_stg_code_review.md` | Все замечания закрыты |

### В `docs/plans/` (1 файл)

| Откуда | Файл | Примечание |
|--------|-------|---------|
| `docs/internal/` | `docs_restructuring_plan.md` | Этот план — активный; после выполнения → `docs/archive/` |

### В `docs/design/` (9 файлов из `docs/internal/`)

| Файл | Роль |
|------|------|
| `PRD.md` | Стратегия продукта |
| `assignment_design.md` | Дизайн курсового задания |
| `naming_conventions.md` | Стандарт нейминга (единый источник) |
| `db_schema.md` | Схема БД всех слоёв DWH |
| `bookings_stg_design.md` | Дизайн STG-слоя |
| `bookings_ods_design.md` | Дизайн ODS-слоя |
| `bookings_dds_design.md` | Дизайн DDS-слоя |
| `bookings_dm_design.md` | Дизайн DM-слоя |
| `architecture_review.md` | Архитектурные решения (ADR) |

### В `docs/reference/` (5 файлов из `docs/internal/`)

| Файл | Роль |
|------|------|
| `pxf_bookings.md` | PXF: настройка, проблемы |
| `bookings_tz.md` | Источник bookings-db |
| `bookings_db_issues.md` | Известные проблемы bookings-db |
| `bookings_generation_benchmark.md` | Бенчмарк генерации |
| `qa-plan.md` | План отладки пайплайна |

**Итог:** `docs/internal/` опустеет → удалить.

---

## Фаза 3. Обновление перекрёстных ссылок

### Категория A. Корневые и публичные файлы (ссылаются на `docs/internal/`)

| Файл | Ссылок | Детали замен |
|------|--------|------|
| `AGENTS.md` | 2 | строки 15, 45: `docs/internal/naming_conventions.md` → `docs/design/naming_conventions.md` |
| `TODO.md` | 6 | строки 6, 7, 43: → `docs/design/`; строка 121: → `docs/reference/pxf_bookings.md`; строка 128: → `docs/reference/bookings_db_issues.md`; строка 141: → `docs/archive/dim_routes_denormalization_plan.md` |
| `docs/README.md` | 7 | строки 18-24: все `internal/*` → `design/*` или `reference/*` |
| `docs/bookings_to_gp_stage.md` | 2 | строка 151: → `reference/pxf_bookings.md`; строка 156: → `archive/bookings_stg_code_review.md` |
| `docs/stack.md` | 1 | строка 85: → `reference/pxf_bookings.md` |

### Категория B. Файлы внутри `docs/design/` и `docs/reference/`

Полный реестр ссылок с `docs/internal/` в файлах, которые переедут в `design/` или
`reference/`. Все требуют обновления — либо кросс-каталожные пути, либо display-тексты.

**B1. Кросс-каталожные ссылки (ссылка ведёт в другой каталог — путь сломается):**

| Файл (→ design/) | Строка | Ссылка | Новый путь |
|------|--------|--------|------|
| `db_schema.md` | 426 | `bookings_tz.md` (отн.) | `../reference/bookings_tz.md` |
| `db_schema.md` | 427 | `pxf_bookings.md` (отн.) | `../reference/pxf_bookings.md` |
| `db_schema.md` | 425 | `bookings_stg_code_review.md` (отн.) | `../archive/bookings_stg_code_review.md` |
| `bookings_stg_design.md` | 5 | `docs/internal/bookings_tz.md` | `../reference/bookings_tz.md` |
| `bookings_stg_design.md` | 130 | `docs/internal/bookings_tz.md` | `../reference/bookings_tz.md` |
| `bookings_stg_design.md` | 131 | `docs/internal/pxf_bookings.md` | `../reference/pxf_bookings.md` |

| Файл (→ reference/) | Строка | Ссылка | Новый путь |
|------|--------|--------|------|
| `qa-plan.md` | 10 | `docs/internal/bookings_db_issues.md` | `bookings_db_issues.md` (тот же каталог) |

**B2. Внутрикаталожные, но с полным путём `docs/internal/...` (путь не сломается
для ссылок с относительным target, но display-текст устареет):**

| Файл (→ design/) | Строки | Что обновить |
|------|--------|------|
| `PRD.md` | 209 | `docs/internal/naming_conventions.md` → `docs/design/naming_conventions.md` |
| `bookings_ods_design.md` | 48 | display-текст `docs/internal/naming_conventions.md` → `docs/design/naming_conventions.md` |
| `bookings_dds_design.md` | 8 | `docs/internal/bookings_ods_design.md` → `docs/design/bookings_ods_design.md` |
| `bookings_dds_design.md` | 185 | display-текст `docs/internal/naming_conventions.md` → `docs/design/naming_conventions.md` |
| `bookings_dds_design.md` | 828-829 | `docs/internal/bookings_dds_design.md`, `docs/internal/db_schema.md` → `docs/design/...` |
| `bookings_dds_design.md` | 945 | `docs/internal/db_schema.md` → `docs/design/db_schema.md` |
| `bookings_dm_design.md` | 279 | `docs/internal/db_schema.md` → `docs/design/db_schema.md` |
| `bookings_dm_design.md` | 283, 291 | `docs/internal/bookings_dm_design.md` → `docs/design/bookings_dm_design.md` |
| `bookings_dm_design.md` | 395 | `docs/internal/naming_conventions.md` → `docs/design/naming_conventions.md` |
| `db_schema.md` | 26 | display-текст `docs/internal/naming_conventions.md` → `docs/design/naming_conventions.md` |
| `db_schema.md` | 421-423 | display-тексты `docs/internal/bookings_*_design.md` → `docs/design/...` |
| `architecture_review.md` | 70 | `docs/internal/bookings_dm_design.md` → `docs/design/bookings_dm_design.md` |
| `architecture_review.md` | 146 | `docs/internal/distribution_strategy.md` → **удалить путь** (файл не существует, оставить как текстовый backlog-пункт без ссылки) |

### Категория C. Ссылки на `educational-tasks.md`

| Файл | Действие |
|------|----------|
| `README.md` (корень) | Заменить ссылку на `educational-tasks.md` → `docs/assignment/` |
| `docs/README.md` | Заменить ссылку на `educational-tasks.md` → `assignment/` |

### Категория D. Файлы в `docs/archive/` — НЕ ТРОГАЕМ

Архивные файлы замораживаются. Ссылки внутри них могут быть битыми — это ожидаемо.

### Комментарий в SQL

`sql/dm/sales_report_ddl.sql` строка 52: `naming_conventions.md` (без пути) —
оставить как есть (комментарий, не ссылка; путь и так неточный).

---

## Фаза 4. Актуализация содержания

| Файл (новый путь) | Что сделать |
|------|-------------|
| `docs/design/architecture_review.md` | DM завершён (5/5 витрин), пометить выполненные P2; строка 146 — убрать путь к несуществующему `distribution_strategy.md`, оставить как текстовый backlog-пункт |
| `docs/design/db_schema.md` | Добавить DM-слой, убрать выполненный TODO |
| `TESTING.md` | Убрать артефакт «Docker-стенд не запускался» |
| `docs/dag_execution_order.md` | Добавить все 4 DDL DAG-а |
| `docs/reference/pxf_bookings.md` | Исправить нумерацию разделов (7→9→8→10 → последовательную) |

---

## Фаза 5. Обновление индексов и заглушка assignment

### `docs/README.md`

Переписать структуру: разделы по каталогам (`design/`, `reference/`, `plans/`,
`archive/`, `assignment/`). Включить ранее пропущенные документы: `db_schema.md`,
`dag_execution_order.md`, `e2e-etl-test-protocol.md`, `agent-dag-testing.md`.

### `README.md` (корень)

Заменить ссылку на `educational-tasks.md` → `docs/assignment/`, проверить остальные.

### `docs/assignment/README.md` (новый файл)

Временная заглушка:
- Указание, что курсовые задания появятся в Этапе 3
- Ссылка на эталонный срез (DAG-и STG→ODS→DDS→DM) для самостоятельного изучения
- Ссылка на `docs/design/assignment_design.md` для менторов

---

## Фаза 6. Финальная проверка

Шаги выполняются строго по порядку:

1. [ ] `make test` — тесты проходят
2. [ ] `make lint` — стиль кода
3. [ ] Удалить пустой каталог `docs/internal/`
4. [ ] Перенести план из `docs/plans/` в `docs/archive/docs_restructuring_plan.md`
5. [ ] grep по `internal/` в живых .md файлах (`rg --glob '!docs/archive/*'`) — нет битых ссылок
6. [ ] grep по `educational-tasks` в живых .md файлах (`rg --glob '!docs/archive/*'`) — нет битых ссылок

---

## Порядок выполнения

Фазы 1→2→3 делаются вместе (иначе ссылки будут битыми).
Фаза 4 — независима, можно параллельно.
Фаза 5 — после всех перемещений.
Фаза 6 — в конце.

---

## Что это даёт следующим этапам

| Этап | Как помогает |
|------|-------------|
| **Этап 2** (подготовка main) | Чистая структура — понятно, что удалять, что оставлять |
| **Этап 3** (ТЗ от аналитика) | Готовый каталог `docs/assignment/` с заглушкой, место для `analyst_spec.md` |
| **Этап 4** (валидационный DAG) | `docs/reference/qa-plan.md` — рядом с другими справочными |
| **Этап 5** (ветка solution) | Архив отделён — не попадёт в ветку solution |
