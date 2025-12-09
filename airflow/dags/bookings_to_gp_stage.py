from __future__ import annotations

"""
Учебный DAG для менти: показывает, как устроен поток
от источника bookings-db до слоя stg в Greenplum.

Важно: этот файл специально должен быть хорошо задокументирован —
docstring и комментарии помогают студенту понять, «зачем» каждая задача,
а не только «что именно она делает».
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from helpers.greenplum import get_gp_conn


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _generate_bookings_day(load_date: str) -> None:
    """
    Генерирует данные за указанный день в bookings-db.

    Важно: функция должна быть идемпотентной:
    если данные за load_date уже есть в исходной БД,
    повторно пересобирать день не нужно.
    """
    logging.info(
        "Генерация учебного дня в bookings-db за дату %s (заглушка)", load_date
    )
    # TODO: реализовать проверку наличия дня в bookings.bookings
    #  и генерацию нового дня при его отсутствии.


def _get_last_loaded_ts_from_gp() -> str | None:
    """
    Возвращает максимальное значение src_created_at_ts из stg.bookings.

    Пока функция возвращает None как заглушку, что соответствует
    режиму полной загрузки (full).
    """
    logging.info(
        "Чтение последнего загруженного src_created_at_ts из stg.bookings (заглушка)"
    )
    # Пример будущей реализации:
    # with get_gp_conn() as conn, conn.cursor() as cur:
    #     cur.execute("SELECT max(src_created_at_ts) FROM stg.bookings")
    #     row = cur.fetchone()
    #     return row[0]
    return None


def _extract_and_load_increment_via_pxf(
    last_loaded_ts: str | None,
    load_date: str,
) -> None:
    """
    Читает дельту из stg.bookings_ext и вставляет её в stg.bookings.

    Логика:
    - если last_loaded_ts is None → первая загрузка (full),
      берём все данные за load_date и ранее;
    - иначе берём только записи, где src_created_at_ts > last_loaded_ts
      и не позже конца учебного дня.
    """
    logging.info(
        "Загрузка инкремента через PXF: last_loaded_ts=%s, load_date=%s (заглушка)",
        last_loaded_ts,
        load_date,
    )
    # TODO: реализовать INSERT INTO stg.bookings (...) SELECT ... FROM stg.bookings_ext
    #  с учётом инкрементального окна по src_created_at_ts.


def _check_row_counts(load_date: str) -> None:
    """
    Проверяет, что количество строк из источника и в stg.bookings совпадает.

    Эта проверка должна помочь студенту увидеть пример простой DQ‑проверки
    для инкрементальной загрузки.
    """
    logging.info("Проверка количества строк за %s (заглушка)", load_date)
    # TODO: реализовать сравнение количества строк,
    #  например через SELECT COUNT(*) в источнике и в stg.bookings.


def _finish_summary() -> None:
    """Логирует краткий итог выполнения DAG за один запуск."""
    logging.info("DAG bookings_to_gp_stage завершён (пока только скелет).")


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
        },
    )

    check_row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=_check_row_counts,
        op_kwargs={"load_date": "{{ ds }}"},
    )

    finish_summary = PythonOperator(
        task_id="finish_summary",
        python_callable=_finish_summary,
    )

    generate_bookings_day >> get_last_loaded_ts >> extract_and_load_increment >> check_row_counts >> finish_summary
