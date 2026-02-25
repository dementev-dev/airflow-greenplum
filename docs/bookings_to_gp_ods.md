# DAG `bookings_to_gp_ods`: `stg` -> `ods` в Greenplum

Этот DAG — учебный пример загрузки типизированного слоя **ODS** из уже подготовленного слоя **STG**.
Логика простая и каноничная: **SCD1 UPSERT** (обновляем изменившиеся записи, вставляем новые) + DQ-проверки.

## Что делает DAG

- Определяет `stg_batch_id`:
  - берёт из `dag_run.conf["stg_batch_id"]`, если передан;
  - иначе берёт последний **согласованный** `batch_id`, который есть во всех snapshot-таблицах STG
    (`airports`, `airplanes`, `routes`, `seats`).
- Загружает 9 таблиц ODS (`airports`, `airplanes`, `routes`, `seats`, `bookings`, `tickets`,
  `flights`, `segments`, `boarding_passes`).
- Для каждой таблицы выполняет пару задач `load -> dq`.
- На загрузке использует дедупликацию внутри батча + UPSERT (SCD1).
- Для snapshot-справочников (`airports`, `airplanes`, `routes`, `seats`) дополнительно
  синхронизирует ключи (удаляет из ODS записи, отсутствующие в выбранном STG-батче).
- Для `flights` дополнительно добирает рейсы из истории `stg.flights`, если на них
  ссылаются `stg.segments` выбранного батча (чтобы сохранить ссылочную целостность
  `segments.flight_id -> flights.flight_id`).

## Что должно быть готово перед запуском

1) Стек поднят:

```bash
make up
```

2) STG-слой создан и заполнен:

- запущен `bookings_stg_ddl` (или `make ddl-gp`);
- хотя бы один раз выполнен DAG `bookings_to_gp_stage`.

3) ODS-таблицы созданы (один из вариантов):

- учебный: запустить DAG `bookings_ods_ddl`;
- шорткат: `make ddl-gp` (в этом проекте он создаёт и STG, и ODS).

## Как запустить

1) Откройте Airflow UI: http://localhost:8080.
2) Запустите DAG `bookings_to_gp_ods`.
3) (Опционально) передайте `stg_batch_id` в конфиге запуска:

```json
{"stg_batch_id": "manual__2026-02-22T12:00:00+00:00"}
```

Если конфиг не передан, DAG автоматически возьмёт последний согласованный snapshot-батч.

## Граф зависимостей (упрощённо)

- `resolve_stg_batch_id`
- Параллельно стартуют ветки:
  - `bookings -> tickets`
  - `airports`
  - `airplanes`
- Далее:
  - `routes` после `airports` и `airplanes`
  - `seats` после `airplanes`
  - `flights` после `routes`
  - `segments` после `flights` и `tickets`
  - `boarding_passes` после `segments`
- Финал: `finish_ods_summary` ждёт `dq_ods_boarding_passes` и `dq_ods_seats`.

## Как проверить результат

```bash
make gp-psql
```

```sql
SELECT COUNT(*) FROM ods.bookings;
SELECT COUNT(*) FROM ods.tickets;
SELECT COUNT(*) FROM ods.flights;

SELECT book_ref, COUNT(*)
FROM ods.bookings
GROUP BY 1
HAVING COUNT(*) > 1;
```

Ожидаемо: в последнем запросе `0` строк.

## Типичные ошибки

- `stg_batch_id не найден`:
  - передайте `stg_batch_id` в `dag_run.conf`, или
  - сначала загрузите STG через `bookings_to_gp_stage`.
- Ошибки `relation "ods...." does not exist`:
  - не применён ODS DDL (`bookings_ods_ddl` / `make ddl-gp`).
- Ошибки DQ по ссылочной целостности:
  - проверьте, что ODS DAG выполнялся с корректным `stg_batch_id` и без пропуска upstream задач.
