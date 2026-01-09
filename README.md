# airflow-dwh-gp-lab

Учебный стенд для лабораторных по Data Engineering: **Airflow** оркестрирует загрузку данных из демо‑БД
**bookings** (Postgres) в **Greenplum**.

## Зачем этот стенд

Основная цель стенда — дать вам удобное место для лабораторных работ и будущей курсовой:
вы построите небольшое аналитическое хранилище данных (DWH) на базе **Greenplum** и закрепите навыки:

- моделирования данных (слои DWH, модели, витрины);
- построения ETL/ELT;
- работы с Airflow и Greenplum.

В курсовой у нас один источник данных — демо‑БД **bookings**. В стенде уже есть готовый учебный пример
загрузки **bookings → stg в Greenplum**, чтобы вы могли сфокусироваться на DWH‑части (ODS/DDS/DM) и
не тратить время на инфраструктуру.

## Что внутри

- **Airflow** (UI: http://localhost:8080, логин/пароль: admin/admin)
- **Greenplum** (single‑node для обучения; внешний порт по умолчанию `5435`)
- **bookings-db** (Postgres с демо‑БД `demo`; внешний порт по умолчанию `5434`)
- **PXF** как “транспорт” между Postgres и Greenplum (уже настроен в образе)
- Побочный пример: загрузка данных через **pandas/CSV** (`csv_to_greenplum`)

## PXF (в 5 строк)

PXF (Platform Extension Framework) — компонент Greenplum для работы с внешними источниками.
В этом стенде PXF используется для чтения таблицы `bookings.bookings` из Postgres прямо из Greenplum
через внешнюю таблицу `stg.bookings_ext`. Поэтому загрузка в `stg.bookings` выглядит как обычный
`INSERT ... SELECT` без промежуточных CSV.
Подробное руководство по PXF смотрите в официальной документации Greenplum.

## Быстрый старт (основной сценарий: bookings → stg)

1) Скопируйте настройки:

```bash
cp .env.example .env
```

2) Поднимите стенд:

```bash
make up
# если make не установлен: docker compose up -d
```

3) Инициализируйте демо‑БД bookings:

```bash
make bookings-init
```

4) Подготовьте STG‑объекты в Greenplum (выберите один вариант):

- Учебный вариант: в Airflow UI запустите DAG `bookings_stg_ddl`;
- Технический шорткат: `make ddl-gp` (применяет все DDL разом вручную).

5) Запустите основной DAG `bookings_to_gp_stage`.

6) Проверьте результат в Greenplum:

```bash
make gp-psql
-- внутри psql:
SELECT COUNT(*) FROM stg.bookings;
SELECT * FROM stg.bookings ORDER BY src_created_at_ts DESC LIMIT 10;
```

Подробнее про логику DAG и проверки — `docs/bookings_to_gp_stage.md`.

## DAG-и в стенде

Основные (для потока bookings → DWH):

- `bookings_stg_ddl` — создаёт `stg.bookings_ext` и `stg.bookings` в Greenplum;
- `bookings_to_gp_stage` — генерирует учебный день в `bookings-db` и грузит инкремент в `stg.bookings`
  (через PXF), затем выполняет DQ‑проверку.

Вспомогательные (побочный трек с CSV):

- `orders_base_ddl` — создаёт таблицу `public.orders` для CSV‑пайплайна;
- `csv_to_greenplum` — pandas → CSV → Greenplum (пример загрузки без источника‑БД);
- `csv_to_greenplum_dq` — проверки качества данных для `public.orders`.

## Полезные команды

```bash
make up                 # поднять стек
make logs               # логи airflow-webserver и airflow-scheduler
make gp-psql            # psql в Greenplum
make bookings-psql      # psql в демо-БД bookings (Postgres)
make ddl-gp             # применить DDL к Greenplum вручную (вместо DDL-DAG)
make down               # остановить и удалить контейнеры/сети (volumes сохраняются)
make clean              # полный reset: удалить контейнеры/сети и volumes (данные будут потеряны)
```

## Подключение через DBeaver (опционально)

Перед подключением убедитесь, что стенд поднят (`make up`).

### Greenplum

- `Host`: `localhost`
- `Port`: `5435`
- `Database`: значение `GP_DB` из `.env` (по умолчанию `gp_dwh`)
- `Username`: значение `GP_USER` (по умолчанию `gpadmin`)
- `Password`: значение `GP_PASSWORD` (по умолчанию `gpadmin`)

### bookings-db (Postgres, демо-БД `demo`)

- `Host`: `localhost`
- `Port`: значение `BOOKINGS_DB_PORT` из `.env` (по умолчанию `5434`)
- `Database`: `demo`
- `Username`: значение `BOOKINGS_DB_USER` (по умолчанию `bookings`)
- `Password`: значение `BOOKINGS_DB_PASSWORD` (по умолчанию `bookings`)

## Документация

- Учебные задания: `educational-tasks.md`).
- План тестирования/проверок и негативные кейсы: `TESTING.md`.
- Дополнительные заметки и технические детали: `docs/README.md`.

## Типичные проблемы и решения

| Проблема | Решение |
|----------|---------|
| Airflow UI не открывается | Дождитесь сообщения `Listening at: http://0.0.0.0:8080` в логах (`make logs`) |
| `database "demo" does not exist` в bookings‑DAG | Вы сделали reset с удалением volumes (`make clean` / `docker compose down -v`). Запустите `make bookings-init` и повторите DAG. |
| Ошибка подключения к Greenplum | Убедитесь, что контейнер `greenplum` имеет статус `healthy` (`docker compose ps`) |
| `protocol "pxf" does not exist` | Перезапустите `greenplum` и повторите `bookings_stg_ddl`/`make ddl-gp` — расширение `pxf` создаётся автоматически при старте контейнера. |
| DAG `bookings_to_gp_stage` ругается на отсутствующие таблицы stg | Запустите `bookings_stg_ddl` (или выполните `make ddl-gp`), затем повторите запуск |
| Порты 8080/5434/5435 заняты | Остановите локальные сервисы или измените порты в `.env`/`docker-compose.yml` |

## Благодарности

- **Postgres Pro** — за демо‑БД bookings и генератор данных `demodb`: https://github.com/postgrespro/demodb (лицензия MIT: https://github.com/postgrespro/demodb/blob/main/LICENSE).
- **woblerr** — за Docker-сборку Greenplum: https://github.com/woblerr/docker-greenplum (образ: `woblerr/greenplum`, лицензия MIT: https://github.com/woblerr/docker-greenplum/blob/master/LICENSE).
