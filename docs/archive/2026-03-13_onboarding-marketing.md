# План: Онбординг студента + маркетинг (v4)

> Статус: выполнен (2026-03-13)

## Контекст

Ментор опционален. Студент, клонировав main, проходит «Быстрый старт» — и застревает:
нет явного «что дальше», ссылка на задания спрятана, validate DAG не объяснён, нет маркетинга.
На main есть противоречия: docs и DAG-docstrings говорят «все реализовано»,
фактически стоят заглушки `SELECT 1;`.

## Что было сделано

### На main (10 файлов)

1. **README.md** — маркетинг-баннер, шаг 6 (только эталонные таблицы), шаг 7 → задания.
2. **docs/assignment/README.md** — полный гид студента (эталон → ТЗ → заглушки → validate).
3. **docs/assignment/analyst_spec.md** — DAG-интеграция: «таски уже подключены, DAG менять не нужно».
4. **docs/bookings_to_gp_dm.md** — пометки заглушек, адаптация секций проверки.
5. **docs/bookings_to_gp_ods.md** — пометки заглушек airplanes/seats.
6. **docs/bookings_to_gp_dds.md** — пометки заглушек dim_routes/dim_passengers/dim_airplanes.
7-10. **4 DAG docstrings** — эталон vs задания (заглушки).

### На solution (follow-up)

- README.md — маркетинг-баннер + шаг 7 (solution-specific: «Изучите готовую реализацию»).
- docs/assignment/README.md — навигационный гид по всем паттернам (ODS → DDS → DM).
- Этот план архивирован в docs/archive/.

## Верификация

- `make test` — passed на обеих ветках.
- `make lint` — clean на обеих ветках.
- Grep «заглушки/SELECT 1» в README.md и docs/assignment/README.md на solution — не найдено.
