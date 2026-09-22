# Проверка лабораторной PXF → STG, 22 сентября 2026

Задача [#9](https://git.dementev.space/ddmitry/airflow-greenplum/issues/9)
из [серии #8](https://git.dementev.space/ddmitry/airflow-greenplum/issues/8).
[Материалы автора](../design/pxf_airports_lab.md) описывают границу
самостоятельной работы и эталонное решение.

## Среда и начальное состояние

Проверена учебная ветка на базе `main` (`1dd5838`) с добавленными
`lab_pxf_airports` и независимой сверкой. Студенческие заглушки ODS/DDS/DM
сохранены. Полная реализация из `solution` в этот стенд не переносилась.
Версии: Airflow 2.9.2, Greenplum 6.27.1, Postgres 16.

```bash
make up
make bookings-init
make ddl-gp
```

После подготовки выполнена обычная цепочка STG → ODS → DDS → DM с Run ID
`lab9-baseline-stage`, `lab9-baseline-ods`, `lab9-baseline-dds`,
`lab9-baseline-dm`. Все четыре запуска завершились успешно до вычитания.

В локальном `.env` использованы свободные порты: Airflow 18090, метабаза
15433, Bookings 15434, Greenplum 15435. Airflow ограничен одним web-worker,
`PARALLELISM=2`, `MAX_ACTIVE_TASKS_PER_DAG=2`, `PARSING_PROCESSES=1`.
Эти настройки не внесены в репозиторий; SQL не изменен.

Первый прогон базового генератора прервался: Postgres сообщил о выходе
server process с кодом 2 и переинициализации, после чего источник оказался
пустым. `OOMKilled=false`, рестарта контейнера не было; доказанной причины
нет. После ограничения ресурсов только своего Airflow источник восстановлен
через `make bookings-init`, а базовая цепочка успешно повторена.
Чужие контейнеры не изменялись. Дальнейшие снимки справочников получены
через `lab_pxf_airports`, без генерации транзакционных дней.

DAG запускались через `POST /api/v1/dags/{dag_id}/dagRuns` с телом
`{"dag_run_id": "указанный в протоколе Run ID", "conf": {}}`.
Перед следующим шагом проверялось состояние `success` всего запуска.

## Вычитание, A и повтор

A/B/C — обозначения авторского прогона. В ученической инструкции это
первая загрузка, новый снимок после переименования и снимок после восстановления.
Файлам `source-A.json` и `source-B.json` в задании соответствуют
`airports-before.json` и `airports-after.json`.

```bash
mkdir -p data/lab9
uv run --no-project scripts/check_airports_snapshot.py export data/lab9/source-A.json
```

Прямой экспорт Postgres: 5501 аэропорт, все шесть бизнес-полей.
Внешний DDL удален из `sql/stg/airports_ddl.sql`, загрузка заменена
на `SELECT 1;`. В Greenplum:

```sql
DROP EXTERNAL TABLE IF EXISTS stg.airports_ext;
TRUNCATE TABLE stg.airports;
SELECT COUNT(*) FROM stg.airports;
```

Получено 0 строк. Заглушка успешно выполнилась, но сверка отказала:

```bash
uv run --no-project scripts/check_airports_snapshot.py check lab9-empty data/lab9/source-A.json
```

Код 1: `Не получены ключи`, всего 5501. Запрос к `stg.airports_ext`
получил `relation does not exist`. Старые данные не маскируют вычитание.
После возврата эталонных DDL и загрузки применен файл:

```sql
\set ON_ERROR_STOP on
\i /sql/stg/airports_ddl.sql
SELECT * FROM stg.airports_ext ORDER BY airport_code LIMIT 3;
```

PXF вернул AAA, AAC, AAE с исходными JSON, координатами и часовыми поясами.
Успешно выполнен `lab_pxf_airports` с Run ID `lab9-A-stage`.

```bash
uv run --no-project scripts/check_airports_snapshot.py check lab9-A-stage data/lab9/source-A.json
```

Код 0: 5501 ключ, все бизнес-поля и метки соответствуют требованиям.
Повторены только `load_airports_to_stg`, затем `check_airports_dq`
в том же запуске через
`POST /api/v1/dags/lab_pxf_airports/clearTaskInstances`:

```json
{
  "dag_run_id": "lab9-A-stage",
  "task_ids": ["load_airports_to_stg"],
  "dry_run": true,
  "only_failed": false,
  "only_running": false,
  "include_upstream": false,
  "include_downstream": false,
  "include_subdags": false,
  "include_parentdag": false,
  "reset_dag_runs": true
}
```

Предварительный ответ содержал ровно одну выбранную задачу, затем
отправлено то же тело с `dry_run=false`. Для DQ заменен `task_ids`.
Повторная сверка A — код 0. При сравнении по ключу все строки, включая
метки, совпали до и после повтора. Базовый генератор не повторялся.

В браузере проверены граф, Run ID и окно Clear task. Подтверждение повтора
выполнялось через API. Первоначальный тестовый сценарий ошибочно сравнивал
порядок строк MPP-ответа. После сравнения по ключу повтор подтвердился;
ошибка не относилась к загрузке. Независимая сверка изначально сопоставляет
строки по ключу, экспорт дополнительно сортирует их для файлового сравнения.

## Изменение источника и неверная копия

В Postgres выполнено:

```sql
CREATE TABLE public.lab9_airport_original AS
SELECT airport_code, airport_name
FROM bookings.airports_data ORDER BY airport_code LIMIT 1;

UPDATE bookings.airports_data AS a
SET airport_name = jsonb_set(
    b.airport_name, '{ru}', to_jsonb((b.airport_name->>'ru') || ' [lab9]'), false
)
FROM public.lab9_airport_original AS b
WHERE a.airport_code = b.airport_code
RETURNING a.airport_code, b.airport_name AS original, a.airport_name AS changed;
```

Изменен один ключ AAA: `Анаа` → `Анаа [lab9]`, английское `Anaa`
и структура JSON сохранены. Внешняя таблица сразу вернула новое имя,
первый снимок сохранил старое.

```bash
uv run --no-project scripts/check_airports_snapshot.py export data/lab9/source-B.json
uv run --no-project scripts/check_airports_snapshot.py check lab9-A-stage data/lab9/source-B.json
```

В источнике по-прежнему 5501 строка. Проверка — код 1,
`AAA: отличается airport_name`. Затем в Greenplum создана старая копия
с новым ID и заполненными метками:

```sql
INSERT INTO stg.airports
SELECT airport_code, airport_name, city, country, coordinates, timezone,
       now(), now(), 'lab9-bad-copy'
FROM stg.airports WHERE _load_id = 'lab9-A-stage';
```

```bash
uv run --no-project scripts/check_airports_snapshot.py check lab9-bad-copy data/lab9/source-B.json
```

Вставлено 5501 строк; проверка отвергла копию с кодом 1 и ошибкой имени.
Контрольные строки удалены:

```sql
DELETE FROM stg.airports WHERE _load_id = 'lab9-bad-copy';
```

## Снимок B и передача в ODS

После успешного `lab_pxf_airports`, Run ID `lab9-B-stage`:

```bash
uv run --no-project scripts/check_airports_snapshot.py check lab9-B-stage data/lab9/source-B.json
uv run --no-project scripts/check_airports_snapshot.py check lab9-A-stage data/lab9/source-A.json
```

Обе сверки прошли. Общий батч проверен запросом:

```sql
SELECT 'airports' AS object, _load_id, COUNT(*) FROM stg.airports WHERE _load_id = 'lab9-B-stage' GROUP BY _load_id
UNION ALL
SELECT 'airplanes', _load_id, COUNT(*) FROM stg.airplanes WHERE _load_id = 'lab9-B-stage' GROUP BY _load_id
UNION ALL
SELECT 'routes', _load_id, COUNT(*) FROM stg.routes WHERE _load_id = 'lab9-B-stage' GROUP BY _load_id
UNION ALL
SELECT 'seats', _load_id, COUNT(*) FROM stg.seats WHERE _load_id = 'lab9-B-stage' GROUP BY _load_id;
```

| Таблица | Строк с `lab9-B-stage` |
|---|---:|
| airports | 5501 |
| airplanes | 10 |
| routes | 860 |
| seats | 1741 |

До запуска ODS команда ниже отказала из-за прежнего батча и названия,
после запуска — прошла:

```bash
uv run --no-project scripts/check_airports_snapshot.py check lab9-B-stage data/lab9/source-B.json --layer ods
```

Выполнен обычный `bookings_to_gp_ods`, Run ID `lab9-B-ods`, `conf={}`.
XCom `return_value` задачи `resolve_stg_batch_id` равен `lab9-B-stage`.
Все 5501 строк ODS совпали с ожидаемыми данными и меткой B;
название AAA — `Анаа [lab9]`. Выбор через `conf` не подменял resolver.

## Восстановление и продолжение

В Postgres дважды выполнен UPDATE восстановления:

```sql
UPDATE bookings.airports_data AS a
SET airport_name = b.airport_name
FROM public.lab9_airport_original AS b
WHERE a.airport_code = b.airport_code
RETURNING a.airport_code, a.airport_name;
```

Оба раза `UPDATE 1`, исходный JSON восстановлен. Файловая проверка:

```bash
uv run --no-project scripts/check_airports_snapshot.py export data/lab9/source-C.json
cmp data/lab9/source-A.json data/lab9/source-C.json
```

Файлы совпали побайтово. Затем успешно выполнены:

| DAG | Run ID |
|---|---|
| lab_pxf_airports | lab9-C-stage |
| bookings_to_gp_ods | lab9-C-ods |
| bookings_to_gp_dds | lab9-C-dds |
| bookings_to_gp_dm | lab9-C-dm |

Resolver с пустым `conf` выбрал `lab9-C-stage`. Сверки A, B и C со своими
источниками, а также всей ODS с C прошли.

```sql
SELECT _load_id, COUNT(*) FROM stg.airports GROUP BY _load_id ORDER BY _load_id;
SELECT airport_bk, airport_name FROM dds.dim_airports ORDER BY airport_bk LIMIT 1;
SELECT COUNT(*) FROM dds.fact_flight_sales;
SELECT COUNT(*) FROM dm.sales_report;
```

В STG остались A/B/C по 5501 строк каждый. В `dds.dim_airports` у AAA
снова `Анаа`. Факт содержит 1 608 696 строк, `dm.sales_report` — 25 732.
Остальные справочники сохраняют также батчи быстрого старта;
транзакционные данные не менялись во время A/B/C. Новый общий снимок
пригоден для заданий `ods.airplanes` и `ods.seats`.
Полный `bookings_validate` не запускался: будущие задания остаются заглушками.

## Проверки кода и документации

- `make lint` — успешно.
- `make test` — 14 passed, 15 skipped: десять тестов сверки и четыре
  SQL-контракта прошли. Четырнадцать smoke-тестов DAG пропущены из-за
  существующих заглушек Airflow в `conftest.py`; один тест ODS требует
  явного включения интеграционного прогона.
- В настоящем Airflow отдельно проверен DagBag: ошибок импорта нет,
  восемь задач нового DAG, SQL-шаблоны раскрываются в исходные файлы,
  зависимости загрузок и DQ верны. REST API не сообщил ошибок импорта.
- Проверены локальные ссылки и актуальность карты. В браузере пройден
  маршрут README → задания → PXF → ODS и ссылка карточки `stg.airports`.
- `git diff --check` пройден в учебной и авторской рабочих копиях.

Учебный прогон выполнен до редакционной перестройки урока. Сценарии,
SQL загрузки, DAG и сверка сохранены; изменились порядок объяснений,
имена файлов в примерах и показ подсказок. После редактирования проверены
296 локальных ссылок учебной ветки и 9 ссылок авторских материалов,
синтаксис bash-примеров, актуальность карты (39 объектов, 90 связей,
216 ссылок). В локальном HTML-предпросмотре Chromium раскрыты все восемь
подсказок, проверено отображение девяти блоков кода и переход к ODS.

Упрощенное завершение урока использует исходный контроль вместо третьего
файла. На сохраненном состоянии прогона отдельно выполнены обе команды:

```bash
uv run --no-project scripts/check_airports_snapshot.py check lab9-C-stage data/lab9/source-A.json
uv run --no-project scripts/check_airports_snapshot.py check lab9-C-stage data/lab9/source-A.json --layer ods
```

Обе прошли: 5501 ключ, все бизнес-поля и метки соответствуют требованиям.
Полный прогон после редакционной правки не повторялся.

## Проверка исправления после ревью

Независимое ревью Standards не обнаружило нарушений. Ревью Spec нашло
ловушку при исправлении SQL: уже записанные дубли остаются в батче после
добавления `NOT EXISTS`. Инструкция дополнена очисткой только ошибочного
`_load_id`, восстановлением снимка и повтором на заполненном снимке.
Исправление повторно просмотрено; замечание закрыто.

В отдельном батче `lab9-review-retry` дважды выполнен INSERT всех строк
`stg.airports_ext` с текущими метками. Получено 11002 строки, сверка отказала
из-за дублей. После выполнения эталонного `airports_load.sql` с этим же
Run ID осталось 11002 строки; сверка снова отказала.

```sql
DELETE FROM stg.airports WHERE _load_id = 'lab9-review-retry';
```

После очистки эталонная загрузка дала 5501 строку и успешную сверку.
Ее повтор также прошел. Временный батч затем удален тем же DELETE;
состав и количества A/B/C сохранились. Это отдельный SQL-опыт восстановления,
а не повтор всего учебного маршрута.

Перед публикацией `make lint` и `make test` снова прошли
(14 passed, 15 skipped). Состояния семи запусков A/B/C и нижних слоев
перечитаны через Airflow API: все `success`. Выдержка настоящего лога
`check_airports_dq` первого запуска:

```text
[2026-09-22T17:42:17.505+0300] {taskinstance.py:1206} INFO - Marking task as SUCCESS. dag_id=lab_pxf_airports, task_id=check_airports_dq, run_id=lab9-A-stage, execution_date=20260922T144212, start_date=20260922T144216, end_date=20260922T144217
[2026-09-22T17:42:17.586+0300] {local_task_job_runner.py:240} INFO - Task exited with return code 0
```
