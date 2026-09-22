# Восстановите прием справочника из источника

Хранилище получает справочник аэропортов из Postgres. Названия и другие
атрибуты в источнике могут меняться, поэтому загрузка должна сохранять
состояние справочника на момент приема данных.

Вы разберете готовый участок `airports` и напишете его заново: создадите
внешнюю таблицу PXF и загрузку полного снимка в STG. Затем испытаете ее
на повторном запуске и изменении источника. Полученный снимок понадобится
следующему слою — ODS.

## Задача

Ваш участок на [карте](../design/architecture-map.html#node=stg.airports):
`bookings.airports_data → stg.airports_ext → stg.airports`.

| Что написать | Где |
|---|---|
| DDL внешней таблицы `stg.airports_ext` | [airports_ddl.sql](../../sql/stg/airports_ddl.sql) |
| Загрузку полного снимка во внутреннюю `stg.airports` | [airports_load.sql](../../sql/stg/airports_load.sql) |

Внутренняя таблица, JDBC-сервер PXF, DAG и проверки качества уже готовы.
Рядом есть пример для другого справочника:
[DDL airplanes](../../sql/stg/airplanes_ddl.sql) и
[его загрузка](../../sql/stg/airplanes_load.sql). Используйте его, учитывая
состав полей и ключ аэропорта.

Требования к вашей загрузке:

- Источник — `bookings.airports_data` в базе `demo`, ключ — `airport_code`.
- Все шесть полей (`airport_code`, `airport_name`, `city`, `country`,
  `coordinates`, `timezone`) попадают в STG без потерь и преобразований.
  Внешняя и внутренняя таблицы используют TEXT; JSON и координаты
  пока хранятся как текст.
- Строки одного снимка получают `_load_id = '{{ run_id }}'`,
  `_load_ts = now()` и `event_ts = now()`. Точного времени изменения
  аэропорта в источнике нет, поэтому `event_ts` — время получения снимка.
- Новый запуск сохраняет новый полный снимок, оставляя предыдущие.
  Повторное выполнение загрузки в том же запуске на неизменном источнике
  не меняет бизнес-данные и не создает дубли ключей внутри снимка.

## Подготовка

Нужен свой стенд после [быстрого старта и знакомства с моделью](README.md#подготовка).
Во время работы не запускайте другие загрузки: они могут изменить данные
посреди опыта. Если запущен `bookings_validate`, дождитесь его завершения.

Сначала освободите участок для своей реализации. Команды ниже удаляют
внешнюю таблицу и **всю историю только `stg.airports`**. Это нужно, чтобы
данные быстрого старта не скрыли отсутствие вашей загрузки.

<details>
<summary>Подготовка файлов и таблиц</summary>

Сохраните свои SQL-файлы в Git или отдельной копии. В `airports_ddl.sql`
удалите блок от `DROP EXTERNAL TABLE` до конца `CREATE EXTERNAL TABLE`;
DDL схемы и внутренней таблицы оставьте. Содержимое `airports_load.sql`
замените на `SELECT 1;`. DAG и DQ менять не нужно.

В Greenplum (`make gp-psql`):

```sql
DROP EXTERNAL TABLE IF EXISTS stg.airports_ext;
TRUNCATE TABLE stg.airports;
SELECT COUNT(*) FROM stg.airports; -- 0
```

Остальные справочники и текущий ODS сохраняются. Пока участок
не восстановлен, ODS запускать не нужно.

</details>

## Соберите загрузку

Начните с внешней таблицы. Добейтесь, чтобы запрос к `stg.airports_ext`
возвращал те же поля и значения, что запрос к `bookings.airports_data`
в Postgres. Для начала достаточно рассмотреть несколько аэропортов.

<details>
<summary>Ориентиры для PXF и применение DDL</summary>

В `LOCATION` укажите таблицу источника, `PROFILE=JDBC` и `SERVER=bookings-db`.
`SERVER` — имя готового [каталога конфигурации PXF](../../pxf/servers/bookings-db/jdbc-site.xml).
Формат чтения — `FORMAT 'CUSTOM' (formatter='pxfwritable_import')`.
Логин и пароль в DDL не нужны. Если меняли учетные данные источника,
согласуйте их с PXF по [справочнику стенда](../stack.md).

Примените свой файл из `make gp-psql`:

```sql
\set ON_ERROR_STOP on
\i /sql/stg/airports_ddl.sql
SELECT * FROM stg.airports_ext ORDER BY airport_code LIMIT 3;
```

В Postgres (`make bookings-psql`) выполните такой же запрос к
`bookings.airports_data`. Сверьте состав и порядок полей с DDL.
Если чтение не работает, начните с сообщения об ошибке и
[диагностики STG](../bookings_to_gp_stage.md#если-загрузка-не-прошла).

</details>

Теперь напишите загрузку снимка. Продумайте, как ваш SQL отличит
повторное выполнение от нового запуска и сохранит историю.
Выполнять загрузку будет готовый DAG `lab_pxf_airports`.

Получите первый снимок и сравните его с источником готовой сверкой.
Она проверяет ключи, все бизнес-поля и служебные метки. Зеленые задачи
в Airflow показывают, что код выполнился; сверка помогает обнаружить
ошибку в самих данных.

<details>
<summary>Первый запуск в Airflow и сверка с Postgres</summary>

Для сверки нужен `uv` с Python 3.11 из
[окружения разработчика](../stack.md#локальное-окружение-разработчика-uv).
В терминале из корня репозитория сохраните исходный справочник:

```bash
mkdir -p data/lab9
uv run --no-project scripts/check_airports_snapshot.py export data/lab9/airports-before.json
```

Скрипт читает Postgres напрямую, минуя вашу внешнюю таблицу, и не перезаписывает
существующий файл. Эта копия позволит сравнить первый снимок с источником
даже после изменения названия аэропорта.

Откройте Airflow UI по адресу из `.env` (`AIRFLOW_WEB_PORT`), войдите с
`AIRFLOW_USER` и `AIRFLOW_PASSWORD`. Включите `lab_pxf_airports`, нажмите
**Trigger DAG** и дождитесь успеха всех задач. В **Grid** выберите запуск;
его **Run ID** подставьте в команду:

```bash
first_run='вставьте Run ID первого запуска'
uv run --no-project scripts/check_airports_snapshot.py check "$first_run" data/lab9/airports-before.json
```

Если задача упала, откройте ее **Log**. SQL-файлы доступны контейнеру
из рабочей папки: после правки пересобирать образ не нужно.
`{{ run_id }}` подставляет Airflow, поэтому файл загрузки не следует
выполнять напрямую через `psql`.

</details>

## Испытайте загрузку

### Повторное выполнение

Представьте, что после сбоя пришлось повторить уже выполненную задачу.
Что произойдет с данными при повторном выполнении вашего SQL? За счет
чего в одном снимке не появится вторая копия каждого аэропорта?

Повторите загрузку **в том же запуске** и снова выполните сверку первого
снимка. При ошибке исправьте SQL и повторите опыт на том же Run ID.

<details>
<summary>Как повторить задачу, сохранив Run ID</summary>

В запуске первого снимка выберите `load_airports_to_stg` → **Clear task**.
Снимите **Upstream**, **Downstream**, **Past**, **Future**, **Recursive**
и **Only Failed**, если включены. В **Affected Tasks** должна остаться
только эта задача этого запуска. Нажмите **Clear**, дождитесь успеха,
затем так же повторите `check_airports_dq`.

Новый **Trigger DAG** создал бы другой Run ID. Здесь нужно повторное
выполнение с прежним `_load_id`, чтобы испытать защиту от дублей.

Если загрузка уже записала дубли или неверные значения, исправление SQL
само их не удалит.
Удалите из `stg.airports` строки только этого `_load_id`, затем повторите
исправленную загрузку и DQ в том же запуске. После успешной сверки повторите
их еще раз: теперь вы проверяете защиту от дублей на заполненном снимке.

</details>

### Изменение в источнике

Теперь аэропорт переименовали. До новой загрузки предположите, где будет
видно новое название: в Postgres, во внешней таблице, в первом снимке STG?

Измените русское название одного аэропорта, сохранив ключ и остальные
языки. Для опыта ниже есть готовый SQL, который также сохраняет исходное
название для восстановления.

<details>
<summary>Изменить название в Postgres</summary>

В `make bookings-psql`:

```sql
\set ON_ERROR_STOP on
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

Запрос изменяет один аэропорт. Если таблица копии уже существует после
предыдущего опыта, сначала [восстановите источник](#завершение-работы),
сверьте результат и удалите таблицу копии. Затем можно начать новый опыт.

</details>

Найдите этот аэропорт в `stg.airports_ext` и `stg.airports` обычными
SELECT-запросами. Сопоставьте наблюдение со своим предположением.

Запустите `lab_pxf_airports` заново и рассмотрите оба снимка в STG.
Какое название хранится в каждом? Почему одного сравнения количества
строк недостаточно, чтобы заметить переименование?

Сверьте новый снимок с измененным источником, а первый — с исходным.
Это обнаружит, например, загрузку старых данных под новым Run ID.

<details>
<summary>Сверить оба снимка</summary>

В том же терминале, где задан `first_run`:

```bash
uv run --no-project scripts/check_airports_snapshot.py export data/lab9/airports-after.json
next_run='вставьте Run ID нового запуска'
uv run --no-project scripts/check_airports_snapshot.py check "$next_run" data/lab9/airports-after.json
uv run --no-project scripts/check_airports_snapshot.py check "$first_run" data/lab9/airports-before.json
```

Если терминал закрывался, значения Run ID можно снова взять из Airflow.

</details>

## Передайте снимок в ODS

Запустите `bookings_to_gp_ods` без параметров `conf`. Найдите измененный
аэропорт в `ods.airports`: название должно быть новым, а `_load_id` —
совпадать с последним запуском STG. Так вы увидите, что следующий слой
использовал результат вашей загрузки.

ODS выбирает общий батч четырех справочников: `airports`, `airplanes`,
`routes`, `seats`. Поэтому готовый `lab_pxf_airports` загружает все четыре,
используя те же SQL, что основной STG-DAG. Генератор новых дней и
транзакционные таблицы в этом опыте не участвуют.

<details>
<summary>Проверить выбор снимка и данные ODS</summary>

В Log задачи `resolve_stg_batch_id` строка `Используем stg_batch_id=...`
показывает выбранный Run ID. Он должен совпасть с `next_run`.
После завершения DAG выполните:

```bash
uv run --no-project scripts/check_airports_snapshot.py check "$next_run" data/lab9/airports-after.json --layer ods
```

Сверка учитывает преобразование JSON в русские названия и требует нужный
`_load_id` у всех строк ODS. Если выбран старый батч, посмотрите, завершились
ли все загрузки и DQ последнего запуска `lab_pxf_airports`.

</details>

## Завершение работы

Верните исходное название и получите еще один общий снимок через
`lab_pxf_airports`. Проведите его по цепочке
`bookings_to_gp_ods` → `bookings_to_gp_dds` → `bookings_to_gp_dm`.
В `ods.airports` и `dds.dim_airports` должно вернуться исходное название.

<details>
<summary>Восстановить источник и сверить результат</summary>

В `make bookings-psql`:

```sql
UPDATE bookings.airports_data AS a
SET airport_name = b.airport_name
FROM public.lab9_airport_original AS b
WHERE a.airport_code = b.airport_code
RETURNING a.airport_code, a.airport_name;
```

После нового запуска `lab_pxf_airports` и ODS сравните восстановленный
снимок с исходной копией:

```bash
restored_run='вставьте Run ID запуска STG после восстановления'
uv run --no-project scripts/check_airports_snapshot.py check "$restored_run" data/lab9/airports-before.json
uv run --no-project scripts/check_airports_snapshot.py check "$restored_run" data/lab9/airports-before.json --layer ods
```

Повтор UPDATE восстановления безопасен. После успешной сверки выполните
в Postgres `DROP TABLE public.lab9_airport_original;`.
Если прервались на изменении источника,
продолжите с этого восстановления и нового запуска цепочки.

Для повторения всей лабораторной возьмите новые имена контрольных файлов.
При полной пересборке сохраните SQL-файлы: `make clean` удаляет тома.
Затем снова пройдите быстрый старт со своим решением.

</details>

Снимки в STG остаются историей опыта: первоначальный, с измененным названием
и восстановленный. Последний общий снимок и текущий ODS снова содержат
исходные значения; транзакционные данные остались от быстрого старта.

Результат работы — ваши два SQL-файла и объяснение их поведения: откуда
берется каждый снимок, что защищает его от дублей при повторе и как
изменение источника попадает в следующую загрузку.

Дальше — [загрузки `ods.airplanes` и `ods.seats`](README.md#первая-загрузка-ods).
Они используют уже подготовленный снимок, поэтому заново запускать STG
для начала следующей работы не нужно. Там вы будете преобразовывать поля
полученных данных и заменять текущее состояние в ODS.
