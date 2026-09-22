"""Готовый DAG первой лабораторной: общий снимок справочников без генерации дней."""

from datetime import timedelta

import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

with DAG(
    dag_id="lab_pxf_airports",
    start_date=pendulum.datetime(2017, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["lab", "bookings", "pxf"],
    description="Лабораторная PXF: четыре снимка STG с общим Run ID для ODS",
) as dag:
    load_airports_to_stg = PostgresOperator(
        task_id="load_airports_to_stg",
        postgres_conn_id="greenplum_conn",
        sql="stg/airports_load.sql",
    )
    check_airports_dq = PostgresOperator(
        task_id="check_airports_dq",
        postgres_conn_id="greenplum_conn",
        sql="stg/airports_dq.sql",
    )
    load_airplanes_to_stg = PostgresOperator(
        task_id="load_airplanes_to_stg",
        postgres_conn_id="greenplum_conn",
        sql="stg/airplanes_load.sql",
    )
    check_airplanes_dq = PostgresOperator(
        task_id="check_airplanes_dq",
        postgres_conn_id="greenplum_conn",
        sql="stg/airplanes_dq.sql",
    )
    load_routes_to_stg = PostgresOperator(
        task_id="load_routes_to_stg",
        postgres_conn_id="greenplum_conn",
        sql="stg/routes_load.sql",
    )
    check_routes_dq = PostgresOperator(
        task_id="check_routes_dq",
        postgres_conn_id="greenplum_conn",
        sql="stg/routes_dq.sql",
    )
    load_seats_to_stg = PostgresOperator(
        task_id="load_seats_to_stg",
        postgres_conn_id="greenplum_conn",
        sql="stg/seats_load.sql",
    )
    check_seats_dq = PostgresOperator(
        task_id="check_seats_dq",
        postgres_conn_id="greenplum_conn",
        sql="stg/seats_dq.sql",
    )

    load_airports_to_stg >> check_airports_dq
    load_airplanes_to_stg >> check_airplanes_dq
    # Готовим один общий батч: DQ маршрутов и мест читает связанные справочники.
    [check_airports_dq, check_airplanes_dq] >> load_routes_to_stg >> check_routes_dq
    check_airplanes_dq >> load_seats_to_stg >> check_seats_dq
