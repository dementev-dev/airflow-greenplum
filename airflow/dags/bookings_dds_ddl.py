from __future__ import annotations

"""
Учебный DAG: создаёт/обновляет слой dds в Greenplum для домена bookings.

Запускается вручную перед DAG загрузки `bookings_to_gp_dds` или после изменения DDS DDL.
Создаёт 7 DDS-таблиц: dim_calendar, dim_airports, dim_airplanes,
dim_tariffs, dim_passengers, dim_routes, fact_flight_sales.
"""

from datetime import timedelta

import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="bookings_dds_ddl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "greenplum", "ddl", "bookings", "dds"],
    description="Учебный DDL DAG: создаёт/обновляет dds.* для bookings",
) as dag:
    apply_dds_dim_calendar_ddl = PostgresOperator(
        task_id="apply_dds_dim_calendar_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_calendar_ddl.sql",
    )

    apply_dds_dim_airports_ddl = PostgresOperator(
        task_id="apply_dds_dim_airports_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_airports_ddl.sql",
    )

    apply_dds_dim_airplanes_ddl = PostgresOperator(
        task_id="apply_dds_dim_airplanes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_airplanes_ddl.sql",
    )

    apply_dds_dim_tariffs_ddl = PostgresOperator(
        task_id="apply_dds_dim_tariffs_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_tariffs_ddl.sql",
    )

    apply_dds_dim_passengers_ddl = PostgresOperator(
        task_id="apply_dds_dim_passengers_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_passengers_ddl.sql",
    )

    apply_dds_dim_routes_ddl = PostgresOperator(
        task_id="apply_dds_dim_routes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/dim_routes_ddl.sql",
    )

    apply_dds_fact_flight_sales_ddl = PostgresOperator(
        task_id="apply_dds_fact_flight_sales_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dds/fact_flight_sales_ddl.sql",
    )

    # DDL применяем последовательно, чтобы порядок был понятным для новичков,
    # а ошибки — воспроизводимыми (в логах сразу видно, на каком объекте упали).
    (
        apply_dds_dim_calendar_ddl
        >> apply_dds_dim_airports_ddl
        >> apply_dds_dim_airplanes_ddl
        >> apply_dds_dim_tariffs_ddl
        >> apply_dds_dim_passengers_ddl
        >> apply_dds_dim_routes_ddl
        >> apply_dds_fact_flight_sales_ddl
    )
