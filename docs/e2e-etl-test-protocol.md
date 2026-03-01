# Протокол сквозного (E2E) тестирования ETL

Этот документ описывает процедуру полной проверки цепочки ETL: `STG -> ODS -> DDS -> DM`.
Цель теста — убедиться в корректности инкрементальной загрузки, работы паттерна `Temporary Table` и механизмов `HWM`.

---

## 1. Подготовка окружения

Убедитесь, что все сервисы запущены и DDL применен.

```bash
make up
make bookings-init
make ddl-gp
```

### Важно: Настройка дат
Для корректного тестирования исторических данных из `demodb` (начинаются с 2017 года), убедитесь, что в DAG-файлах `start_date` установлен в `2017-01-01`.

---

## 2. Очистка данных (Reset)

Перед началом теста необходимо полностью очистить все слои DWH.

```bash
make dwh-truncate
```

---

## 3. Этап 1: Загрузка за первый день (2017-01-01)

Выполните последовательный запуск всех DAG для первой порции данных.

```bash
# Загрузка в STG (создает первый батч в источнике)
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_stage 2017-01-01

# Загрузка в ODS (Initial Load)
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_ods 2017-01-01

# Загрузка в DDS (Initial Load)
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_dds 2017-01-01

# Загрузка в DM (Initial Load витрины)
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_dm 2017-01-01
```

### Ожидаемые результаты (Day 1)
Проверьте наполнение таблиц:
- `stg.bookings` и `ods.bookings` должны иметь одинаковое количество строк (>0).
- `dm.sales_report` должна содержать агрегированные данные за первый день.

---

## 4. Этап 2: Проверка инкремента (2017-01-02)

Эмулируйте появление данных за второй день и проверьте дозагрузку.

```bash
# Генерация данных за 2-й день в базе-источнике
make bookings-generate-day

# Повторный запуск цепочки ETL
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_stage 2017-01-02
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_ods 2017-01-02
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_dds 2017-01-02
docker compose exec airflow-scheduler airflow dags test bookings_to_gp_dm 2017-01-02
```

---

## 5. Финальная верификация (Критерии успеха)

Выполните SQL-запрос для сверки данных:

```bash
make gp-psql -c "
SELECT 'STG' as layer, COUNT(*) FROM stg.bookings
UNION ALL
SELECT 'ODS' as layer, COUNT(*) FROM ods.bookings
UNION ALL
SELECT 'DDS' as layer, COUNT(*) FROM dds.fact_flight_sales
UNION ALL
SELECT 'DM ' as layer, COUNT(*) FROM dm.sales_report;
"
```

**Критерии корректности:**
1. **STG == ODS**: Количество строк в `stg.bookings` и `ods.bookings` совпадает (т.к. это SCD1 UPSERT).
2. **Инкремент STG**: Количество строк в `stg.bookings` после Day 2 больше, чем после Day 1.
3. **Инкремент ODS (Temporary Table)**: В ODS нет дублей. `SELECT book_ref FROM ods.bookings GROUP BY book_ref HAVING COUNT(*) > 1` должен вернуть 0 строк.
4. **HWM в DM**: Витрина `sales_report` содержит данные за оба дня. Значение `COUNT(*)` после Day 2 должно вырасти по сравнению с Day 1.
5. **Lineage**: Поля `_load_id` и `_load_ts` во всех слоях содержат метки соответствующих запусков.

---

## Типичные ошибки
- **Пустые таблицы**: Проверьте, что в `bookings-db` есть данные (`SELECT COUNT(*) FROM bookings.bookings`). Если 0 — сделайте `make bookings-init`.
- **Пропуски в ODS**: Убедитесь, что `stg_batch_id` в ODS корректно вычисляется (задача `resolve_stg_batch_id`).
- **Дубли в DDS**: Проверьте логику генерации SK в `dds/*_load.sql`.
