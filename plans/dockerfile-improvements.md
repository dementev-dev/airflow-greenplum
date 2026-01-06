# План улучшения Dockerfile и docker-compose.yml

## Обзор

Документ описывает план улучшения Dockerfile для Airflow и его интеграции с docker-compose.yml на основе анализа best practices.

## Согласованные изменения

✅ Переименовать `Dockerfile` → `Dockerfile.airflow`
✅ Обновить `build: .` → `build: { context: ., dockerfile: Dockerfile.airflow }`
✅ Добавить YAML anchors для устранения дублирования конфигурации
✅ Добавить healthcheck для airflow-webserver
✅ Исправить расположение requirements.txt в Dockerfile
✅ Добавить LABEL в Dockerfile
✅ Добавить `USER airflow` после установки зависимостей
✅ Добавить проверку `pip check`
✅ Переименовать контейнеры (`gp_airflow_web` → `gp_airflow_webserver`, `gp_airflow_sch` → `gp_airflow_scheduler`)

---

## Часть 1: Изменения в Dockerfile

### Текущее состояние (Dockerfile)
```dockerfile
FROM apache/airflow:2.9.2

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

### Новое состояние (Dockerfile.airflow)
```dockerfile
# Apache Airflow с дополнительными зависимостями для Greenplum
FROM apache/airflow:2.9.2

LABEL maintainer="your-email@example.com"
LABEL description="Airflow with Greenplum and Pandas dependencies"
LABEL version="1.0"

# Копируем requirements в стандартное расположение
COPY airflow/requirements.txt /opt/airflow/requirements.txt

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && pip check \
    && rm -rf /tmp/pip-*

# Переключаемся на пользователя airflow
USER airflow

WORKDIR /opt/airflow
```

### Обоснование изменений:

1. **LABEL** - стандартная практика для документирования образов
2. **`/opt/airflow/requirements.txt`** - стандартное расположение для Airflow
3. **`pip check`** - проверка совместимости установленных пакетов
4. **`USER airflow`** - безопасность и соответствие best practices
5. **`WORKDIR /opt/airflow`** - явное указание рабочей директории

---

## Часть 2: Изменения в docker-compose.yml

### Основные изменения:

1. **Добавить YAML anchors** для устранения дублирования
2. **Обновить build** для всех Airflow сервисов
3. **Добавить healthcheck** для airflow-webserver
4. **Добавить комментарии** для пояснения сокращений в именах контейнеров

### Структура YAML anchors:

```yaml
x-airflow-common-env: &airflow-env
  TZ: ${TZ:-Europe/Moscow}
  AIRFLOW__CORE__LOAD_EXAMPLES: "False"
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${PG_USER}:${PG_PASSWORD}@pgmeta:5432/${PG_DB}
  AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW__WEBSERVER__SECRET_KEY}
  AIRFLOW_CONN_GREENPLUM_CONN: postgresql://${GP_USER}:${GP_PASSWORD}@greenplum:${GP_PORT:-5432}/${GP_DB}
  AIRFLOW_CONN_BOOKINGS_DB: postgresql://${BOOKINGS_DB_USER}:${BOOKINGS_DB_PASSWORD}@bookings-db:5432/demo

x-airflow-common-volumes: &airflow-volumes
  - ./airflow/dags:/opt/airflow/dags
  - ./sql:/sql:ro
  - airflow_data:/opt/airflow/data

x-airflow-common-depends: &airflow-depends
  pgmeta:
    condition: service_healthy
  greenplum:
    condition: service_healthy
  airflow-init:
    condition: service_completed_successfully
```

### Изменения для airflow-webserver:

```yaml
airflow-webserver:
  build:
    context: .
    dockerfile: Dockerfile.airflow
  image: airflow-custom:latest
  container_name: gp_airflow_webserver
  env_file: .env
  environment:
    <<: *airflow-env
  command: airflow webserver
  ports:
    - "8080:8080"
  volumes:
    <<: *airflow-volumes
    # requirements.txt монтируется как volume для удобства разработки
    # При изменении зависимостей не требуется пересборка образа
    - ./airflow/requirements.txt:/opt/airflow/requirements.txt
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
    interval: 30s
    timeout: 10s
    retries: 5
    start_period: 40s
  depends_on:
    <<: *airflow-depends
```

### Изменения для airflow-scheduler:

```yaml
airflow-scheduler:
  build:
    context: .
    dockerfile: Dockerfile.airflow
  image: airflow-custom:latest
  container_name: gp_airflow_scheduler
  env_file: .env
  environment:
    <<: *airflow-env
  command: airflow scheduler
  volumes:
    <<: *airflow-volumes
    - ./airflow/requirements.txt:/opt/airflow/requirements.txt
  depends_on:
    <<: *airflow-depends
```

### Изменения для airflow-init:

```yaml
airflow-init:
  build:
    context: .
    dockerfile: Dockerfile.airflow
  image: airflow-custom:latest
  # Имя контейнера закомментировано (одноразовый сервис)
  user: "0"
  env_file: .env
  environment:
    <<: *airflow-env
  volumes:
    <<: *airflow-volumes
  command: >
    bash -lc "
      set -e;
      mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data;
      for i in {1..30}; do
        su -s /bin/bash airflow -c \"PATH='/home/airflow/.local/bin:$${PATH}' airflow db migrate\" && break || echo 'waiting for pgmeta' && sleep 3;
      done;
      su -s /bin/bash airflow -c \"PATH='/home/airflow/.local/bin:$${PATH}' airflow users create --username ${AIRFLOW_USER} --password ${AIRFLOW_PASSWORD} --firstname Admin --lastname User --role Admin --email admin@example.org\" || true
    "
  depends_on:
    pgmeta:
      condition: service_healthy
```

---

## Часть 3: Влияние переименования контейнеров

### Важно: Переименование контейнеров

При переименовании контейнеров (`gp_airflow_web` → `gp_airflow_webserver`, `gp_airflow_sch` → `gp_airflow_scheduler`) необходимо учитывать:

1. **Имена сервисов в docker-compose.yml остаются без изменений**
   - `airflow-webserver`, `airflow-scheduler`, `airflow-init` - это имена сервисов
   - Они используются в командах `docker-compose logs`, `docker-compose restart` и т.д.
   - Эти имена НЕ меняются

2. **Имена контейнеров меняются**
   - `gp_airflow_web` → `gp_airflow_webserver`
   - `gp_airflow_sch` → `gp_airflow_scheduler`
   - Они используются в командах `docker exec`, `docker inspect`, `docker logs`

3. **Проверка использования старых имён контейнеров**
   ```bash
   # Поиск упоминаний старых имён в скриптах
   grep -r "gp_airflow_web" .
   grep -r "gp_airflow_sch" .
   ```

4. **Обновление Makefile (если используется)**
   - Проверить команды, которые используют старые имена контейнеров
   - Пример: `make logs` может использовать `docker-compose logs airflow-webserver` (это корректно)

5. **Обновление документации**
   - Обновить все упоминания имён контейнеров в README.md, TESTING.md
   - Обновить примеры команд в документации

---

## Часть 4: План тестирования изменений

### Этап 1: Подготовка окружения

1. **Остановить текущие контейнеры**
   ```bash
   make down
   ```

2. **Удалить старые образы (опционально)**
   ```bash
   docker rmi airflow-custom:latest
   ```

3. **Проверить наличие .env файла**
   ```bash
   ls -la .env
   # Если отсутствует, скопировать из .env.example
   cp .env.example .env
   ```

### Этап 2: Сборка новых образов

1. **Собрать образы с новым Dockerfile**
   ```bash
   docker-compose build
   ```

2. **Проверить успешность сборки**
   ```bash
   docker images | grep airflow-custom
   ```

3. **Проверить наличие LABEL**
   ```bash
   docker inspect airflow-custom:latest | grep -A 10 "Labels"
   ```

### Этап 3: Запуск сервисов

1. **Запустить весь стек**
   ```bash
   make up
   ```

2. **Проверить статус контейнеров**
   ```bash
   docker-compose ps
   ```

3. **Проверить логи инициализации**
   ```bash
   docker-compose logs airflow-init
   ```

### Этап 4: Проверка healthcheck

1. **Проверить healthcheck для airflow-webserver**
   ```bash
   docker inspect gp_airflow_webserver | grep -A 20 "Health"
   ```

2. **Дождаться healthy статуса**
   ```bash
   watch -n 2 'docker inspect --format="{{.State.Health.Status}}" gp_airflow_webserver'
   ```

### Этап 5: Функциональное тестирование

1. **Проверить доступ к Airflow UI**
   - Открыть http://localhost:8080
   - Проверить авторизацию (использовать креды из .env)
   - Проверить наличие DAG'ов в списке

2. **Проверить запуск DAG'ов**
   - Выбрать любой DAG (например, `csv_to_greenplum`)
   - Запустить вручную через UI
   - Проверить успешность выполнения задач

3. **Проверить подключения к БД**
   - Проверить Admin → Connections
   - Убедиться, что `greenplum_conn` и `bookings_db` доступны
   - Проверить Test Connection для каждого подключения

4. **Проверить логи scheduler**
   ```bash
   docker-compose logs airflow-scheduler | tail -50
   ```

### Этап 6: Проверка использования старых имён контейнеров

1. **Поиск упоминаний старых имён в проекте**
   ```bash
   grep -r "gp_airflow_web" . --exclude-dir=.git --exclude-dir=__pycache__
   grep -r "gp_airflow_sch" . --exclude-dir=.git --exclude-dir=__pycache__
   ```

2. **Проверка Makefile**
   ```bash
   grep -E "gp_airflow_web|gp_airflow_sch" Makefile
   ```

3. **Проверка документации**
   ```bash
   grep -r "gp_airflow_web" README.md TESTING.md AGENTS.md docs/
   grep -r "gp_airflow_sch" README.md TESTING.md AGENTS.md docs/
   ```

4. **Обновление найденных упоминаний**
   - Заменить `gp_airflow_web` на `gp_airflow_webserver`
   - Заменить `gp_airflow_sch` на `gp_airflow_scheduler`
   - Обновить примеры команд в документации

### Этап 7: Тестирование требований

1. **Проверить установленные пакеты**
   ```bash
   docker exec gp_airflow_webserver pip list | grep -E "psycopg2|pandas"
   ```

2. **Проверить совместимость пакетов**
   ```bash
   docker exec gp_airflow_webserver pip check
   ```

3. **Проверить пользователя внутри контейнера**
   ```bash
   docker exec gp_airflow_webserver whoami
   # Ожидаемый результат: airflow
   ```

### Этап 8: Тестирование изменений requirements.txt

1. **Добавить тестовый пакет в requirements.txt**
   ```bash
   echo "requests==2.31.0" >> airflow/requirements.txt
   ```

2. **Перезапустить контейнеры**
   ```bash
   docker-compose restart airflow-webserver airflow-scheduler
   ```

3. **Проверить установку пакета**
   ```bash
   docker exec gp_airflow_webserver pip list | grep requests
   ```

4. **Удалить тестовый пакет**
   ```bash
   # Вернуть исходный requirements.txt
   git checkout airflow/requirements.txt
   ```

### Этап 9: Регрессионное тестирование

1. **Запустить существующие тесты**
   ```bash
   make test
   ```

2. **Проверить smoke-тесты DAG**
   ```bash
   uv run pytest tests/test_dags_smoke.py -v
   ```

3. **Проверить тесты helpers**
   ```bash
   uv run pytest tests/test_greenplum_helpers.py -v
   ```

### Этап 10: Проверка после перезапуска

1. **Полный перезапуск стека**
   ```bash
   make down
   make up
   ```

2. **Проверить сохранность данных**
   - Проверить наличие DAG'ов в UI
   - Проверить историю запусков DAG'ов
   - Проверить подключения к БД

3. **Проверить логи на наличие ошибок**
   ```bash
   docker-compose logs airflow-webserver | grep -i error
   docker-compose logs airflow-scheduler | grep -i error
   # Обратите внимание: имена сервисов не изменились, только имена контейнеров
   ```

---

## Часть 5: Критерии успеха

### Функциональные требования:

- ✅ Все контейнеры успешно запускаются
- ✅ Airflow UI доступен на http://localhost:8080
- ✅ Авторизация работает корректно
- ✅ Все DAG'ы отображаются в UI
- ✅ DAG'и успешно выполняются
- ✅ Подключения к Greenplum и bookings-db работают
- ✅ Healthcheck для airflow-webserver работает корректно

### Технические требования:

- ✅ Образ собирается без ошибок
- ✅ LABEL присутствуют в образе
- ✅ Пакеты устанавливаются от пользователя airflow
- ✅ `pip check` не возвращает ошибок
- ✅ requirements.txt монтируется как volume
- ✅ YAML anchors работают корректно
- ✅ Нет дублирования конфигурации

### Требования к совместимости:

- ✅ Существующие тесты проходят успешно
- ✅ Данные сохраняются после перезапуска
- ✅ История запусков DAG'ов сохраняется
- ✅ Подключения к БД работают как раньше

---

## Часть 6: Откат изменений

Если изменения вызывают проблемы, план отката:

1. **Остановить контейнеры**
   ```bash
   make down
   ```

2. **Вернуть старые файлы**
   ```bash
   git checkout Dockerfile docker-compose.yml
   ```

3. **Удалить новый образ**
   ```bash
   docker rmi airflow-custom:latest
   ```

4. **Перезапустить стек**
   ```bash
   make up
   ```

---

## Часть 7: Документация

После успешного внедрения изменений необходимо обновить:

1. **README.md** - добавить информацию о новом Dockerfile
2. **AGENTS.md** - обновить инструкции для агентов
3. **Комментарии в docker-compose.yml** - добавить пояснения к YAML anchors

---

## Резюме

План включает:
- 2 файла для изменения: `Dockerfile` → `Dockerfile.airflow`, `docker-compose.yml`
- 10 этапов тестирования
- 12 функциональных критериев успеха
- План отката на случай проблем
- Переименование контейнеров для улучшения читаемости
- Проверка использования старых имён контейнеров в проекте

### Важные замечания:

1. **Имена сервисов НЕ меняются** - `airflow-webserver`, `airflow-scheduler`, `airflow-init`
2. **Имена контейнеров меняются** - `gp_airflow_web` → `gp_airflow_webserver`, `gp_airflow_sch` → `gp_airflow_scheduler`
3. **Необходимо проверить** использование старых имён контейнеров в скриптах и документации
4. **Makefile команды** используют имена сервисов, поэтому они продолжат работать без изменений

Все изменения следуют best practices для Docker и Airflow, сохраняют обратную совместимость и не требуют изменений в DAG'ах.
