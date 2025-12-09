# Временное ТЗ по PXF для чтения данных из bookings (черновик)

_Этот файл внутренний, удалить перед итоговой сдачей._

## 1. Цель и границы

- Минимальная цель: настроить PXF в контейнере Greenplum так, чтобы из базы `bookings` (Postgres в сервисе `bookings-db`) можно было делать `SELECT` по одной внешней таблице в Greenplum.
- На этом этапе **не** делаем загрузку в постоянные таблицы Greenplum, только чтение и ручные smoke‑проверки.
- Изменения в коде/конфигурации пока планируем «на бумаге»; реализацию и правки `docker-compose.yml`/SQL/DAG делаем отдельным шагом.

## 2. Архитектура на уровне контейнеров

- `bookings-db` — Postgres 16, демо‑БД `demo` из репозитория `demodb` (источник). Доступен внутри сети Docker по имени `bookings-db` и порту `5432`.
- `greenplum` — контейнер `woblerr/greenplum:6.27.1` (GPDB 6, Ubuntu 22.04). В нём уже есть:
  - Greenplum в режиме singlenode;
  - установленный PXF (`/usr/local/pxf`, `pxf version release-6.10.1`);
  - стартовый скрипт `/start_gpdb.sh`, который умеет включать PXF по флагу `GREENPLUM_PXF_ENABLE=true`.
- Внешний мир (IDE/pytest) подключается к Greenplum по порту `5435` на хосте (см. `docker-compose.yml`), а к `bookings-db` — по порту `${BOOKINGS_DB_PORT}` (см. `.env`).

## 3. Включение PXF в нашем стенде (дизайн)

Планируемые изменения (позже будут внесены в `docker-compose.yml`):

- В сервисе `greenplum` в секцию `environment` добавить:
  - `GREENPLUM_PXF_ENABLE: "true"`.
- При первом старте с этим флагом скрипт `/start_gpdb.sh` сделает за нас:
  - инициализацию PXF (`pxf cluster prepare`, `pxf cluster register`, `pxf cluster sync`);
  - создание расширения `pxf` в базе `${GREENPLUM_DATABASE_NAME}` (у нас это `${GP_DB}`, по умолчанию `gp_dwh`);
  - запуск `pxf cluster start` и привязку остановки/старта PXF к жизненному циклу Greenplum.
- База конфигов PXF (`PXF_BASE`) будет располагаться в `${GREENPLUM_DATA_DIRECTORY}/pxf`, в нашем compose — это `/data/pxf` на томе `greenplum_data`.
  - Важно: `make down` сейчас делает `docker compose down -v`, поэтому при полном сбросе томов будут теряться и данные GP, и конфиги PXF (включая JDBC‑драйвер и `servers/*`).

## 4. JDBC‑драйвер для Postgres: где и как хранить

Задача: PXF должен уметь ходить по JDBC в `bookings-db` (Postgres). Для этого нужен PostgreSQL JDBC драйвер (`postgresql-*.jar`).

Варианты хранения драйвера:

1. **Коммитить JAR в репозиторий** и монтировать в контейнер.
   - Плюсы: стенд самодостаточен, не зависит от внешних скачиваний, повторяемость выше (особенно на офлайн‑машинах или при падении зеркал).
   - Минусы: лишний бинарник в учебном репо, периодически нужно обновлять версию.
2. **Скачивать JAR внутрь контейнера один раз вручную** и хранить его в томе `greenplum_data` внутри `PXF_BASE/lib`.
   - Плюсы: нет бинарников в Git.
   - Минусы: дополнительный шаг для студентов, зависимость от сети, нужно повторять после полного сброса томов.

Для учебного стенда окончательно выбираем вариант **(1) — JAR в репозитории**:

- В репозитории заводим каталог, например `pxf/` или `pxf/jdbc/`, и кладём туда файл `postgresql-42.7.3.jar` (фиксируем версию 42.7.3 как актуальную на момент разработки).
- В `docker-compose.yml` (на этапе реализации) смонтируем этот JAR внутрь контейнера `greenplum` в каталог `$PXF_BASE/lib`, например:
  - `./pxf/postgresql-42.7.3.jar:/data/pxf/lib/postgresql-jdbc.jar:ro`.
- PXF по документации поддерживает размещение JDBC‑драйвера в `$PXF_BASE/lib` (общий для всех серверов) или в `$PXF_BASE/servers/<server>/lib` (локальный для сервера). Для простоты используем общий каталог `$PXF_BASE/lib`.
- Для студентов не будет лишних подготовительных шагов: после `make up` и инициализации конфигов PXF драйвер уже на месте.

Договорённость для реализации:

- Путь в репозитории: условно `pxf/postgresql-42.7.3.jar`.
- Путь внутри контейнера: `/data/pxf/lib/postgresql-jdbc.jar` (через bind‑mount, read‑only).
- Обновление драйвера в будущем — ручная операция (заменить JAR в `pxf/` и скорректировать путь в `docker-compose.yml` при необходимости).

## 5. Сервер PXF для bookings-db (jdbc-site.xml)

PXF использует концепцию «серверов» (`servers/<имя>`), где для каждого сервера хранится свой конфиг подключения (в т.ч. JDBC).

План:

- Создать сервер с именем `bookings-db` (название привязываем к сервису Docker, чтобы не путаться).
- Конфиг храним в репозитории, например в файле:
  - `pxf/servers/bookings-db/jdbc-site.xml`.
- В контейнере этот файл будет доступен как:
  - `/data/pxf/servers/bookings-db/jdbc-site.xml` (bind‑mount read‑only).
- Внутри прописываем параметры подключения к демо‑БД `demo` в Postgres `bookings-db`.

Черновой шаблон `jdbc-site.xml` (значения логина/пароля берём из `.env.example`, блок `BOOKINGS_DB_*` — для учебного стенда допускаем хардкод тех же дефолтных значений):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

  <property>
    <name>jdbc.driver</name>
    <value>org.postgresql.Driver</value>
  </property>

  <property>
    <name>jdbc.url</name>
    <value>jdbc:postgresql://bookings-db:5432/demo</value>
  </property>

  <property>
    <name>jdbc.user</name>
    <value>${BOOKINGS_DB_USER}</value>
  </property>

  <property>
    <name>jdbc.password</name>
    <value>${BOOKINGS_DB_PASSWORD}</value>
  </property>

</configuration>
```

Замечания:

- В реальном файле `pxf/servers/bookings-db/jdbc-site.xml` логин и пароль будут прописаны строками, совпадающими с дефолтами из `.env.example` (`BOOKINGS_DB_USER=bookings`, `BOOKINGS_DB_PASSWORD=bookings`). Это упрощает старт стенда для студентов.
- Если студент поменяет креды `BOOKINGS_DB_USER`/`BOOKINGS_DB_PASSWORD` в своём `.env`, он должен **также** поменять их в `pxf/servers/bookings-db/jdbc-site.xml`, иначе PXF не сможет подключиться к источнику.
- Адрес `bookings-db:5432` — имя сервиса и внутренний порт Postgres внутри сети `docker compose`. Внешний порт (`${BOOKINGS_DB_PORT:-5434}`) здесь не используется.
- При стандартном сценарии (конфиг монтируется read‑only) PXF подхватывает `jdbc-site.xml` при первой инициализации/старте. Если конфиг внутри контейнера всё‑таки меняли вручную, для надёжности можно выполнить:
  ```bash
  pxf cluster sync
  pxf cluster restart
  ```
  чтобы PXF подхватил новые настройки.

## 6. Внешняя таблица в Greenplum (только для чтения)

Задача: завести одну external‑таблицу в Greenplum, которая читает данные из демо‑БД `demo` через PXF/JDBC.

Дизайн:

- Имя таблицы в Greenplum: `public.ext_bookings_bookings` (подчёркиваем, что это внешнее представление таблицы `bookings.bookings` из Postgres).
- Схема колонок должна совпадать со схемой исходной таблицы в `demo` (её нужно будет аккуратно выписать отдельным шагом, через `\d bookings.bookings` в `bookings-db`).
- Локация PXF:
  - `PROFILE=JDBC` — используем JDBC‑профиль.
  - `SERVER=bookings-db` — имя сервера из `jdbc-site.xml`.

Черновой шаблон DDL (без конкретных типов, заполним позже по реальной схеме; предполагается, что финальный DDL ляжет в `sql/ddl_gp.sql`, чтобы применяться через `make ddl-gp`):

```sql
CREATE EXTERNAL TABLE public.ext_bookings_bookings (
    -- TODO: колонки как в bookings.bookings (будет уточнено)
)
LOCATION ('pxf://bookings.bookings?PROFILE=JDBC&SERVER=bookings-db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```

Комментарии:

- На этапе реализации нужно будет:
  - в `bookings-db` посмотреть структуру основного факт‑табличного объекта (скорее всего `bookings.bookings`) и перенести DDL;
  - проверить типы дат/чисел, чтобы избежать сюрпризов на стороне GP.
- Для MVP достаточно одной таблицы; позже можно добавить ещё 1–2 внешние таблицы для примеров (например, справочники).

## 7. План тестирования (без дополнительных make‑таргетов)

Цель тестов: показать студентам, что PXF настроен корректно и позволяет читать данные; при этом не плодить отдельные `make`‑цели, а использовать уже существующие (`make up`, `make bookings-init`, `make gp-psql`).

### 7.1. Позитивный сценарий (smoke)

Предварительные условия:

- `.env` скопирован из `.env.example` и не изменял дефолтные креды для `bookings-db` (`BOOKINGS_DB_USER=bookings`, `BOOKINGS_DB_PASSWORD=bookings`).
- В `docker-compose.yml` включён PXF (`GREENPLUM_PXF_ENABLE=true` в сервисе `greenplum`).
- Для `bookings-db` уже выполнен `make bookings-init` (есть данные в `demo`).
- JAR драйвера (`pxf/postgresql-42.7.3.jar`) и файл `jdbc-site.xml` (`pxf/servers/bookings-db/jdbc-site.xml`) присутствуют в репозитории (они будут автоматически смонтированы в `/data/pxf/lib` и `/data/pxf/servers/bookings-db`).
- DDL внешней таблицы `public.ext_bookings_bookings` добавлен в `sql/ddl_gp.sql` и применяется через `make ddl-gp`.

Шаги (в будущем попадут в `TESTING.md`):

1. Поднять стенд:
   - `make up`
   - дождаться healthcheck‑ов `pgmeta` и `greenplum`.
2. Инициализировать Airflow (если ещё не делали):
   - `make airflow-init`.
3. Подготовить демо‑БД bookings:
   - `make bookings-init`.
4. Применить DDL в Greenplum (создать таблицы, включая внешнюю `public.ext_bookings_bookings`):
   - `make ddl-gp`.
5. Зайти в Greenplum:
   - `make gp-psql`.
6. Проверить, что расширение PXF присутствует:
   - `\dx pxf`.
7. Выполнить простые запросы:
   - `SELECT COUNT(*) FROM public.ext_bookings_bookings;`
   - `SELECT * FROM public.ext_bookings_bookings LIMIT 5;`

Ожидаемый результат:

- Запросы выполняются без ошибок, возвращают ненулевое количество строк.
- Структура данных визуально совпадает с данными в `bookings-db` (можно дополнительно открыть `bookings-psql` и сравнить).

### 7.2. Негативный сценарий (отказ источника)

Цель: показать, как выглядит ошибка, если источник недоступен, и что с этим делать.

Шаги:

1. При работающем стенде остановить только `bookings-db`:
   - `docker compose stop bookings-db`.
2. В `make gp-psql` попробовать снова:
   - `SELECT 1 FROM public.ext_bookings_bookings LIMIT 1;`

Ожидаемый результат:

- Запрос падает с ошибкой подключения к Postgres (через JDBC/pxf).
- В `TESTING.md` планируем добавить короткую подсказку: «если видите ошибку подключения — убедитесь, что запущен сервис `bookings-db` (`docker compose start bookings-db`) и повторите запрос».

### 7.3. Идея для автоматического smoke‑теста (на будущее)

На будущее (не в рамках текущего этапа) можно добавить простой e2e‑тест в `tests/`, который:

- с помощью `psycopg2` коннектится к Greenplum (`GP_*` из `.env`);
- выполняет `SELECT 1 FROM public.ext_bookings_bookings LIMIT 1`;
- помечен как «integration» и запускается только по явному желанию (например, через отдельный маркер или переменную окружения).

Пока это остаётся идеей: сначала реализуем базовую конфигурацию PXF и ручной smoke‑чек‑лист.

## 8. Открытые вопросы / TODO

- Уточнить целевую таблицу(ы) в `demo` для внешнего представления (скорее всего `bookings.bookings`), аккуратно выписать DDL и обновить шаблон из раздела 6.
- При переносе DDL проверить типы дат/времени, чтобы не получить неожиданный сдвиг по часовому поясу (см. `docs/internal/bookings_tz.md`).
- При необходимости добавить интеграционный тест по мотивам раздела 7.3 (по отдельному маркеру/флагу).

## 9. Практические детали и нюансы

- **Таймзона**:
  - Для единообразия логов и данных задаём `TZ=Europe/Moscow` (GMT+3) в `.env.example` и пробрасываем эту переменную в контейнеры `pgmeta`, `bookings-db`, `greenplum`, `airflow-webserver`, `airflow-scheduler`.
  - При проверке данных через PXF имеет смысл сравнивать выборки по времени между `bookings-db` и Greenplum, опираясь на договорённости из `docs/internal/bookings_tz.md`.
- **Поведение при `make down`**:
  - `make down` вызывает `docker compose down -v`, что удаляет все тома, включая `greenplum_data` (`/data` в контейнере).
  - При следующем `make up` Greenplum и PXF будут инициализироваться с нуля, но:
    - JAR и `jdbc-site.xml` возьмутся из репозитория и снова смонтируются в `/data/pxf/...`;
    - `make ddl-gp` снова создаст внешнюю таблицу `public.ext_bookings_bookings`.
  - То есть после полного ресета студенту достаточно повторить цепочку `make up` → `make airflow-init` → `make bookings-init` → `make ddl-gp`.
- **Где искать логи при проблемах с PXF**:
  - Логи PXF: в контейнере `greenplum` под пользователем `gpadmin` в каталоге `${PXF_BASE}/logs` (по умолчанию `/data/pxf/logs`).
  - Логи Greenplum: в `${GREENPLUM_DATA_DIRECTORY}/master/.../pg_log` (например, `/data/master/gpseg-1/pg_log` для GP6).
  - При ошибках подключения к `bookings-db` полезно:
    - проверить, что контейнер `bookings-db` работает (`docker compose ps`);
    - сверить креды в `.env` и `pxf/servers/bookings-db/jdbc-site.xml`;
    - посмотреть сообщения в `/data/pxf/logs`.
