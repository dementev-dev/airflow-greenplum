from __future__ import annotations

"""
Учебный DAG для менти: показывает, как устроен поток
от источника bookings-db до слоя stg в Greenplum.

В этом примере мы сознательно используем:
- Airflow Connections для подключения к БД;
- PostgresOperator, который берёт SQL-скрипты с диска;
чтобы студент увидел «канонический» способ работы с SQL в DAG.
"""

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


BOOKINGS_CONN_ID = "bookings_db"
GREENPLUM_CONN_ID = "greenplum_conn"


def _finish_summary() -> None:
    """
    Логирует краткий итог выполнения DAG за один запуск.

    Здесь можно добавить дополнительную агрегацию/логирование,
    но для учебного примера достаточно простого сообщения.
    """
    from logging import getLogger

    log = getLogger(__name__)
    log.info("DAG bookings_to_gp_stage завершён. Подробности смотрите в логах задач.")


with DAG(
    dag_id="bookings_to_gp_stage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "stg"],
    description="Учебный DAG: загрузка из bookings-db в stg.bookings (Greenplum)",
) as dag:
    # 1. Генерируем один учебный день в демо-БД bookings (идемпотентно)
    generate_bookings_day = PostgresOperator(
        task_id="generate_bookings_day",
        postgres_conn_id=BOOKINGS_CONN_ID,
        sql="/sql/src/bookings_generate_day_if_missing.sql",
        params={"load_date": "{{ ds }}"},
    )

    # 2. Загружаем инкремент из stg.bookings_ext в stg.bookings
    load_bookings_to_stg = PostgresOperator(
        task_id="load_bookings_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="/sql/stg/bookings_load.sql",
        params={
            "load_date": "{{ ds }}",
            "batch_id": "{{ ds_nodash }}",
        },
    )

    # 3. Проверяем количество строк между источником и stg.bookings
    check_row_counts = PostgresOperator(
        task_id="check_row_counts",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="/sql/stg/bookings_dq.sql",
        params={
            "load_date": "{{ ds }}",
            "batch_id": "{{ ds_nodash }}",
        },
    )

    # 4. Финальный лог/сводка
    finish_summary = PythonOperator(
        task_id="finish_summary",
        python_callable=_finish_summary,
    )

    generate_bookings_day >> load_bookings_to_stg >> check_row_counts >> finish_summary
