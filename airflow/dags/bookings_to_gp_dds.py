from __future__ import annotations

"""
Учебный DAG: загрузка из ODS в DDS (Greenplum) по домену bookings.

Ключевая идея:
- DDS читает текущее состояние ODS;
- для каждой сущности выполняем пару задач load -> dq;
- для dim_routes применяем SCD2, для остальных измерений — SCD1 UPSERT;
- факт грузим инкрементальным UPSERT по зерну (ticket_no, flight_id).
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
    """Логирует краткий итог выполнения DDS-ветки."""
    log.info("DAG bookings_to_gp_dds завершён. Подробности смотрите в логах задач.")


with DAG(
    dag_id="bookings_to_gp_dds",
    start_date=pendulum.datetime(2017, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "dds"],
    description="Учебный DAG: загрузка ODS -> DDS (Star Schema) + DQ проверки",
) as dag:
    load_dds_dim_calendar = PostgresOperator(
        task_id="load_dds_dim_calendar",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_calendar_load.sql",
    )

    dq_dds_dim_calendar = PostgresOperator(
        task_id="dq_dds_dim_calendar",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_calendar_dq.sql",
    )

    load_dds_dim_airports = PostgresOperator(
        task_id="load_dds_dim_airports",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_airports_load.sql",
    )

    dq_dds_dim_airports = PostgresOperator(
        task_id="dq_dds_dim_airports",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_airports_dq.sql",
    )

    load_dds_dim_airplanes = PostgresOperator(
        task_id="load_dds_dim_airplanes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_airplanes_load.sql",
    )

    dq_dds_dim_airplanes = PostgresOperator(
        task_id="dq_dds_dim_airplanes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_airplanes_dq.sql",
    )

    load_dds_dim_tariffs = PostgresOperator(
        task_id="load_dds_dim_tariffs",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_tariffs_load.sql",
    )

    dq_dds_dim_tariffs = PostgresOperator(
        task_id="dq_dds_dim_tariffs",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_tariffs_dq.sql",
    )

    load_dds_dim_passengers = PostgresOperator(
        task_id="load_dds_dim_passengers",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_passengers_load.sql",
    )

    dq_dds_dim_passengers = PostgresOperator(
        task_id="dq_dds_dim_passengers",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_passengers_dq.sql",
    )

    load_dds_dim_routes = PostgresOperator(
        task_id="load_dds_dim_routes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_routes_load.sql",
    )

    dq_dds_dim_routes = PostgresOperator(
        task_id="dq_dds_dim_routes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_routes_dq.sql",
    )

    load_dds_fact_flight_sales = PostgresOperator(
        task_id="load_dds_fact_flight_sales",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/fact_flight_sales_load.sql",
    )

    dq_dds_fact_flight_sales = PostgresOperator(
        task_id="dq_dds_fact_flight_sales",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/fact_flight_sales_dq.sql",
    )

    finish_dds_summary = PythonOperator(
        task_id="finish_dds_summary",
        python_callable=_finish_summary,
    )

    load_dds_dim_calendar >> dq_dds_dim_calendar

    dq_dds_dim_calendar >> [
        load_dds_dim_airports,
        load_dds_dim_airplanes,
        load_dds_dim_tariffs,
        load_dds_dim_passengers,
        load_dds_dim_routes,
    ]

    load_dds_dim_airports >> dq_dds_dim_airports
    load_dds_dim_airplanes >> dq_dds_dim_airplanes
    load_dds_dim_tariffs >> dq_dds_dim_tariffs
    load_dds_dim_passengers >> dq_dds_dim_passengers
    load_dds_dim_routes >> dq_dds_dim_routes

    [
        dq_dds_dim_airports,
        dq_dds_dim_airplanes,
        dq_dds_dim_tariffs,
        dq_dds_dim_passengers,
        dq_dds_dim_routes,
    ] >> load_dds_fact_flight_sales

    load_dds_fact_flight_sales >> dq_dds_fact_flight_sales >> finish_dds_summary
