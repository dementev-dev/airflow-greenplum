# Проблемы bookings-db (demodb)

> Дата обнаружения: 2026-03-08
> Зафиксированный коммит demodb: `866e56f7fe54596a1d2a88f5f32f4aa3b2698121`

---

## 1. gen.connstr без credentials — генерация обрывается

### Симптом

После `make bookings-generate` (генерация с нуля) таблицы в demo-базе существуют, но **пустые**.
Повторные `make bookings-generate-day` тоже дают 0 строк.

### Корневая причина

`install.sql` из demodb хардкодит:

```sql
ALTER DATABASE demo SET gen.connstr = 'dbname=demo';
```

Без `user` и `password`. Makefile устанавливает правильный connstr
(с `user=bookings password=bookings`) **после** `install.sql`, но при повторном
`make bookings-generate` порядок тот же: install.sql перезаписывает → Makefile
восстанавливает. Если что-то идёт не так между этими шагами, connstr остаётся
без credentials.

Далее цепочка:
1. `process_queue()` обрабатывает события (BOOKING, FLIGHT, VACUUM и т.д.)
2. Событие VACUUM вызывает `dblink_exec(gen.connstr, 'VACUUM ANALYZE')`
3. dblink без user → подключается как `postgres` → `FATAL: role "postgres" does not exist`
4. `process_queue()` ловит ошибку, логирует и **прекращает работу**
5. Генерация обрывается на 1-2 модельных днях → данных почти нет

### Диагностика

```sql
-- В psql к demo-базе:
SHOW gen.connstr;
-- Если "dbname=demo" без user → проблема подтверждена

SELECT * FROM gen.log ORDER BY at DESC LIMIT 5;
-- Искать: "could not establish connection" / "role postgres does not exist"
```

### Воркэраунд

```sql
-- Выполнить на bookings-db ПОСЛЕ install.sql (актуально при make bookings-generate):
ALTER DATABASE demo SET gen.connstr = 'dbname=demo user=bookings password=bookings';
-- Затем переподключиться к demo и запустить генерацию заново.
```

### Правильное решение

Один из вариантов:
- Патч `install.sql`, подставляющий credentials из ENV
- Обновление demodb до версии, где это исправлено

---

## 2. BOOKINGS_INIT_DAYS=1 — недостаточно для генерации данных

### Симптом

Даже при исправленном connstr, с `BOOKINGS_INIT_DAYS=1` (дефолт в Makefile)
`bookings.bookings` остаётся пустой.

### Корневая причина

Генератор demodb использует константы:
- `ROUTES_DURATION() = 1 month` — маршруты строятся на месяц вперёд
- `ROUTES_TAKE_EFFECT() = 2 months` — маршруты начинают действовать через 2 месяца

При `start_date=2017-01-01` и `init_days=1`:
- `end_date = 2017-01-02`
- Генератор успевает только INIT + BUILD ROUTES (маршруты на февраль-март)
- До создания бронирований не доходит → 0 строк

Проверено:
| init_days | bookings | boarding_passes | Время генерации |
|-----------|----------|-----------------|-----------------|
| 1         | 0        | 0               | ~1 сек          |
| 30        | ~30 000  | 0               | ~1-2 мин        |
| 90        | ?        | ? (ожидаем >0)  | ~10+ мин        |

---

## 3. boarding_passes всегда пустая

### Симптом

Таблица `bookings.boarding_passes` ни разу не содержала данных.

### Корневая причина

Boarding passes создаются при событиях CHECK-IN и BOARDING, которые
генерируются при REGISTRATION рейса (~24ч до вылета). Рейсы начинаются
с 2017-02-01. Цепочка:

1. Нужны маршруты → появляются при init_days ≥ 1
2. Нужны рейсы → появляются при init_days ≥ 30
3. Нужны бронирования/сегменты → появляются при init_days ≥ 30
4. Нужна регистрация (за ~24ч до вылета) → init_days ≥ ~60
5. Нужны boarding events → init_days ≥ ~60

При init_days ≤ 30 генератор не доходит до дат регистрации.
При init_days ≥ 60 баг #1 (connstr) убивал генерацию раньше.

Возможно, есть и баг в самой версии demodb — требует проверки после
обновления.

---

## 4. UX: время генерации

Генерация данных занимает значительное время:

| init_days | Время      | Достаточно для        |
|-----------|------------|-----------------------|
| 30        | ~1-2 мин   | bookings, но не bp    |
| 60        | ~5 мин     | предположительно bp   |
| 90        | ~10+ мин   | всех таблиц (?)      |

Для студента ждать 10 мин при первом запуске — плохой UX.

### Альтернативы

1. **Готовый SQL-дамп** первых N дней (`pg_dump` → `pg_restore`, секунды)
2. **Docker image с предзаполненной БД** (ещё быстрее, 0 ожидания)
3. **Уменьшить масштаб** — найти параметры demodb для меньшего числа аэропортов/маршрутов

---

## Рекомендации (TODO)

- [x] Обновить demodb до последнего коммита (`866e56f`) — upstream только README-правки, баги не исправлены
- [x] Добавить патч для gen.connstr (`install_connstr_no_hardcode.patch`) — убирает хардкод без credentials
- [x] Увеличить BOOKINGS_INIT_DAYS до 60 (в Makefile и .env)
- [x] Добавить валидацию после init (count > 0 для bookings, flights; вывод counts всех таблиц)
- [ ] Решить проблему UX с временем генерации (дамп или prebuilt image)
- [ ] Проверить, появляются ли boarding_passes при init_days=60 + исправленном connstr (нужен запуск стенда)
