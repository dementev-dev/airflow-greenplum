from __future__ import annotations

"""
Учебный DAG: загрузка из DDS в DM (Greenplum) по домену bookings.

Ключевая идея:
- DM читает из DDS (Star Schema);
- для каждой витрины выполняем пару задач load -> dq;
- все витрины загружаются параллельно (не зависят друг от друга);
- sales_report использует UPSERT (heap-таблица с UPDATE).

На данном этапе реализована только эталонная витрина sales_report.
Остальные витрины будут добавлены в последующих этапах.
"""

from datetime import timedelta
from logging import getLogger

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

log = getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _finish_summary() -> None:
    """Логирует краткий итог выполнения DM-ветки."""
    log.info("DAG bookings_to_gp_dm завершён. Подробности смотрите в логах задач.")


with DAG(
    dag_id="bookings_to_gp_dm",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "dm"],
    description="Учебный DAG: загрузка DDS -> DM (Data Mart) + DQ проверки",
) as dag:
    # start-задача (для структуры, вдруг понадобятся pre-checks)
    start_dm = PythonOperator(
        task_id="start_dm",
        python_callable=lambda: log.info("Начало загрузки DM-слоя..."),
    )

    # === Эталонная витрина: sales_report ===
    load_dm_sales_report = PostgresOperator(
        task_id="load_dm_sales_report",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dm/sales_report_load.sql",
    )

    dq_dm_sales_report = PostgresOperator(
        task_id="dq_dm_sales_report",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dm/sales_report_dq.sql",
    )

    # === Заглушки для будущих витрин (будут реализованы в этапах 2-5) ===
    # load_dm_route_performance = PostgresOperator(...)
    # dq_dm_route_performance = PostgresOperator(...)
    # load_dm_passenger_loyalty = PostgresOperator(...)
    # dq_dm_passenger_loyalty = PostgresOperator(...)
    # load_dm_airport_traffic = PostgresOperator(...)
    # dq_dm_airport_traffic = PostgresOperator(...)
    # load_dm_monthly_overview = PostgresOperator(...)
    # dq_dm_monthly_overview = PostgresOperator(...)

    # finish-задача
    finish_dm_summary = PythonOperator(
        task_id="finish_dm_summary",
        python_callable=_finish_summary,
    )

    # Зависимости: параллельные ветки load -> dq
    # В будущем: start_dm >> [load_sales >> dq_sales, load_route >> dq_route, ...] >> finish
    start_dm >> load_dm_sales_report >> dq_dm_sales_report >> finish_dm_summary
