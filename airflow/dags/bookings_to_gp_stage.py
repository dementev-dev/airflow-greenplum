from __future__ import annotations

"""
Учебный DAG для менти: показывает, как устроен поток
от источника bookings-db до слоя stg в Greenplum.

Важно: этот файл специально должен быть хорошо задокументирован —
docstring и комментарии помогают студенту понять, «зачем» каждая задача,
а не только «что именно она делает».
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from helpers.greenplum import get_gp_conn


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


BOOKINGS_CONN_ID = os.getenv("BOOKINGS_CONN_ID", "bookings_db")


def _get_bookings_conn():
    """
    Возвращает подключение к демо-БД bookings.

    Приоритет:
    1. Airflow Connection с ID из BOOKINGS_CONN_ID (по умолчанию: bookings_db)
    2. Прямое подключение по переменным окружения (фоллбек)
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=BOOKINGS_CONN_ID)
        conn = hook.get_conn()
        logging.info(
            "✅ Подключение к bookings-db через Airflow Connection '%s' успешно",
            BOOKINGS_CONN_ID,
        )
        return conn
    except Exception as exc:  # pragma: no cover - фоллбек для нестандартных окружений
        logging.warning(
            "⚠️ Не удалось подключиться к bookings-db через Airflow Connection '%s': %s",
            BOOKINGS_CONN_ID,
            exc,
        )
        logging.info("🔄 Пробуем прямое подключение по переменным окружения")

    import psycopg2

    conn_params = {
        # Внутри Docker-сети bookings-db доступен по имени сервиса и порту 5432
        "host": os.getenv("BOOKINGS_DB_HOST", "bookings-db"),
        "port": int(os.getenv("BOOKINGS_DB_PORT_INTERNAL", "5432")),
        "dbname": os.getenv("BOOKINGS_DB_NAME", "demo"),
        "user": os.getenv("BOOKINGS_DB_USER", "bookings"),
        "password": os.getenv("BOOKINGS_DB_PASSWORD", "bookings"),
    }
    logging.info(
        "🔗 Подключение к bookings-db по ENV: %s:%s/%s",
        conn_params["host"],
        conn_params["port"],
        conn_params["dbname"],
    )
    return psycopg2.connect(**conn_params)


def _generate_bookings_day(load_date: str) -> None:
    """
    Готовит данные за указанный день в bookings-db.

    Идея:
    - если за load_date уже есть строки в bookings.bookings → ничего не делаем;
    - если нет → запускаем генератор (аналог make bookings-generate-day).
    """
    logging.info("Запускаем подготовку данных в bookings-db за дату %s", load_date)

    with _get_bookings_conn() as conn, conn.cursor() as cur:
        # 1. Проверяем, что демо-БД установлена (таблица bookings.bookings существует)
        logging.info("Проверяем наличие таблицы bookings.bookings...")
        cur.execute("SELECT to_regclass('bookings.bookings')")
        table_regclass = cur.fetchone()[0]
        if table_regclass is None:
            raise ValueError(
                "❌ Таблица bookings.bookings не найдена. "
                "Сначала выполните make bookings-init, чтобы подготовить демо-БД."
            )

        # 2. Проверяем, есть ли уже данные за нужный день
        logging.info(
            "Проверяем, есть ли данные за %s в bookings.bookings...", load_date
        )
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM bookings.bookings
                WHERE book_date::date = %s::date
            )
            """,
            (load_date,),
        )
        has_day = bool(cur.fetchone()[0])

        if has_day:
            logging.info(
                "Данные за %s уже есть в bookings.bookings — "
                "генерация не требуется (идемпотентность).",
                load_date,
            )
            return

        logging.info(
            "Данных за %s нет — запускаем генератор демобазы "
            "(аналог make bookings-generate-day)...",
            load_date,
        )

        # 3. Запускаем генерацию следующего дня через тот же DO-блок,
        #    который используется в скрипте bookings/generate_next_day.sql.
        #    Это гарантирует, что логика совпадает с CLI-сценарием.
        cur.execute(
            """
            DO $$
            DECLARE
                v_max_book_date timestamptz;
                v_start_date    timestamptz;
                v_end_date      timestamptz;
                v_jobs          integer := COALESCE(current_setting('bookings.jobs', true), '1')::integer;
                v_init_days     integer := COALESCE(current_setting('bookings.init_days', true), '1')::integer;
                v_start_cfg     text    := COALESCE(current_setting('bookings.start_date', true), '2017-01-01');
            BEGIN
                -- Проверяем, что демобаза установлена
                IF to_regclass('bookings.bookings') IS NULL THEN
                    RAISE EXCEPTION 'Таблица bookings.bookings не найдена. Сначала выполните make bookings-init.';
                END IF;

                -- Ищем последнюю сгенерированную дату
                SELECT max(book_date) INTO v_max_book_date FROM bookings.bookings;

                IF v_max_book_date IS NULL THEN
                    -- База пустая: берём стартовую дату из конфигурации (или дефолтную)
                    v_start_date := date_trunc('day', v_start_cfg::timestamptz);
                ELSE
                    -- Продолжаем с дня, следующего за максимальной датой
                    v_start_date := date_trunc('day', v_max_book_date) + interval '1 day';
                END IF;

                -- Первая генерация вызывает generate(), последующие — continue()
                IF v_max_book_date IS NULL THEN
                    v_end_date := v_start_date + (v_init_days || ' days')::interval;
                    CALL generate(v_start_date, v_end_date, v_jobs);
                ELSE
                    v_end_date := v_start_date + interval '1 day';
                    CALL continue(v_end_date, v_jobs);
                END IF;

                -- Ждём завершения фоновых джобов генератора, чтобы данные успели записаться
                WHILE busy() LOOP
                    PERFORM pg_sleep(1);
                END LOOP;
                PERFORM dblink_disconnect(unnest(dblink_get_connections()));
            END $$;
            """
        )
        conn.commit()

    logging.info("Генерация данных за %s в bookings-db завершена.", load_date)


def _get_last_loaded_ts_from_gp() -> str | None:
    """
    Возвращает максимальное значение src_created_at_ts из stg.bookings.

    Если данных ещё нет, возвращает None — это будет означать
    режим полной загрузки (full).
    """
    with get_gp_conn() as conn, conn.cursor() as cur:
        logging.info("Проверяем наличие таблицы stg.bookings в Greenplum...")
        cur.execute("SELECT to_regclass('stg.bookings')")
        table_regclass = cur.fetchone()[0]
        if table_regclass is None:
            raise ValueError(
                "❌ Таблица stg.bookings не найдена. "
                "Убедитесь, что выполнен DDL для схемы stg (например, make ddl-gp)."
            )

        logging.info(
            "Читаем максимальное значение src_created_at_ts из stg.bookings..."
        )
        cur.execute("SELECT max(src_created_at_ts) FROM stg.bookings")
        row = cur.fetchone()
        last_ts = row[0]

        if last_ts is None:
            logging.info(
                "В stg.bookings пока нет данных — будет выполнена полная загрузка (full)."
            )
            return None

        logging.info(
            "Последний загруженный src_created_at_ts в stg.bookings: %s", last_ts
        )
        # Возвращаем строку, чтобы её было проще использовать в шаблонах и XCom
        return last_ts.isoformat()


def _extract_and_load_increment_via_pxf(
    last_loaded_ts: str | None,
    load_date: str,
    batch_id: str,
) -> None:
    """
    Читает дельту из stg.bookings_ext и вставляет её в stg.bookings.

    Логика:
    - если last_loaded_ts is None → первая загрузка (full),
      берём все данные из источника;
    - иначе берём только записи, где src_created_at_ts > last_loaded_ts
      и не позже конца учебного дня.
    """
    logging.info(
        "Загрузка инкремента через PXF: last_loaded_ts=%s, load_date=%s, batch_id=%s",
        last_loaded_ts,
        load_date,
        batch_id,
    )

    with get_gp_conn() as conn, conn.cursor() as cur:
        if last_loaded_ts is None:
            # Полная загрузка: переносим все строки из внешней таблицы.
            logging.info("Режим загрузки: full (первичная загрузка данных).")
            cur.execute(
                """
                INSERT INTO stg.bookings (
                    book_ref,
                    book_date,
                    total_amount,
                    src_created_at_ts,
                    load_dttm,
                    batch_id
                )
                SELECT
                    book_ref::text,
                    book_date::text,
                    total_amount::text,
                    book_date::timestamp,
                    now(),
                    %s
                FROM stg.bookings_ext
                """,
                (batch_id,),
            )
        else:
            # Инкрементальная загрузка: берём только «новые» строки по окну времени.
            logging.info("Режим загрузки: delta (инкрементальная загрузка).")
            cur.execute(
                """
                INSERT INTO stg.bookings (
                    book_ref,
                    book_date,
                    total_amount,
                    src_created_at_ts,
                    load_dttm,
                    batch_id
                )
                SELECT
                    book_ref::text,
                    book_date::text,
                    total_amount::text,
                    book_date::timestamp,
                    now(),
                    %s
                FROM stg.bookings_ext
                WHERE book_date > %s::timestamp
                  AND book_date <= (%s::date + INTERVAL '1 day')
                """,
                (batch_id, last_loaded_ts, load_date),
            )

        inserted = cur.rowcount if cur.rowcount not in (None, -1) else None
        conn.commit()

    logging.info("Вставлено строк в stg.bookings: %s", inserted)


def _check_row_counts(
    load_date: str,
    last_loaded_ts: str | None,
    batch_id: str,
) -> None:
    """
    Проверяет, что количество строк из источника и в stg.bookings совпадает.

    Для наглядности считаем:
    - количество строк в stg.bookings_ext за текущее окно;
    - количество строк в stg.bookings с текущим batch_id.
    """
    logging.info(
        "Проверка количества строк за %s (last_loaded_ts=%s, batch_id=%s)",
        load_date,
        last_loaded_ts,
        batch_id,
    )

    # Здесь мы специально не вытаскиваем rowcount из предыдущей задачи,
    # а пересчитываем окно, чтобы показать связь DQ‑логики с бизнес-правилами.
    with get_gp_conn() as conn, conn.cursor() as cur:
        if last_loaded_ts is None:
            # full: считаем все строки во внешней таблице
            cur.execute("SELECT COUNT(*) FROM stg.bookings_ext")
            src_count = cur.fetchone()[0]
        else:
            # delta: считаем строки только за текущий интервал
            cur.execute(
                """
                SELECT COUNT(*)
                FROM stg.bookings_ext
                WHERE book_date > %s::timestamp
                  AND book_date <= (%s::date + INTERVAL '1 day')
                """,
                (last_loaded_ts, load_date),
            )
            src_count = cur.fetchone()[0]

        # Считаем количество строк, реально вставленных в stg.bookings в этом запуске
        cur.execute(
            """
            SELECT COUNT(*)
            FROM stg.bookings
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        stg_count = cur.fetchone()[0]

    if src_count != stg_count:
        raise ValueError(
            "❌ Несовпадение количества строк при загрузке bookings: "
            f"источник={src_count}, stg={stg_count}. "
            "Проверьте логи задач extract_and_load_increment_via_pxf "
            "и корректность окна инкремента."
        )

    logging.info(
        "✅ Проверка количества строк пройдена: источник=%s, stg=%s",
        src_count,
        stg_count,
    )


def _finish_summary() -> None:
    """Логирует краткий итог выполнения DAG за один запуск."""
    logging.info("DAG bookings_to_gp_stage завершён.")


with DAG(
    dag_id="bookings_to_gp_stage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "stg"],
    description="Учебный DAG: загрузка из bookings-db в stg.bookings (Greenplum)",
) as dag:
    generate_bookings_day = PythonOperator(
        task_id="generate_bookings_day",
        python_callable=_generate_bookings_day,
        op_kwargs={"load_date": "{{ ds }}"},
    )

    get_last_loaded_ts = PythonOperator(
        task_id="get_last_loaded_ts_from_gp",
        python_callable=_get_last_loaded_ts_from_gp,
    )

    extract_and_load_increment = PythonOperator(
        task_id="extract_and_load_increment_via_pxf",
        python_callable=_extract_and_load_increment_via_pxf,
        op_kwargs={
            "last_loaded_ts": "{{ ti.xcom_pull(task_ids='get_last_loaded_ts_from_gp') }}",
            "load_date": "{{ ds }}",
            "batch_id": "{{ ds_nodash }}",
        },
    )

    check_row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=_check_row_counts,
        op_kwargs={
            "load_date": "{{ ds }}",
            "last_loaded_ts": "{{ ti.xcom_pull(task_ids='get_last_loaded_ts_from_gp') }}",
            "batch_id": "{{ ds_nodash }}",
        },
    )

    finish_summary = PythonOperator(
        task_id="finish_summary",
        python_callable=_finish_summary,
    )

    generate_bookings_day >> get_last_loaded_ts >> extract_and_load_increment >> check_row_counts >> finish_summary

