from __future__ import annotations

"""
Учебный DAG: создаёт/обновляет слой ods в Greenplum для домена bookings.

Запускается вручную перед DAG загрузки `bookings_to_gp_ods` или после изменения ODS DDL.
Создаёт 9 ODS-таблиц: airports, airplanes, routes, seats, bookings, tickets,
flights, segments, boarding_passes.
"""

from datetime import timedelta

import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="bookings_ods_ddl",
    start_date=pendulum.datetime(2017, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "greenplum", "ddl", "bookings", "ods"],
    description="Учебный DDL DAG: создаёт/обновляет ods.* для bookings",
) as dag:
    apply_ods_airports_ddl = PostgresOperator(
        task_id="apply_ods_airports_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/airports_ddl.sql",
    )

    apply_ods_airplanes_ddl = PostgresOperator(
        task_id="apply_ods_airplanes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/airplanes_ddl.sql",
    )

    apply_ods_routes_ddl = PostgresOperator(
        task_id="apply_ods_routes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/routes_ddl.sql",
    )

    apply_ods_seats_ddl = PostgresOperator(
        task_id="apply_ods_seats_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/seats_ddl.sql",
    )

    apply_ods_bookings_ddl = PostgresOperator(
        task_id="apply_ods_bookings_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/bookings_ddl.sql",
    )

    apply_ods_tickets_ddl = PostgresOperator(
        task_id="apply_ods_tickets_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/tickets_ddl.sql",
    )

    apply_ods_flights_ddl = PostgresOperator(
        task_id="apply_ods_flights_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/flights_ddl.sql",
    )

    apply_ods_segments_ddl = PostgresOperator(
        task_id="apply_ods_segments_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/segments_ddl.sql",
    )

    apply_ods_boarding_passes_ddl = PostgresOperator(
        task_id="apply_ods_boarding_passes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/boarding_passes_ddl.sql",
    )

    # DDL применяем последовательно, чтобы порядок был понятным для новичков,
    # а ошибки — воспроизводимыми (в логах сразу видно, на каком объекте упали).
    (
        apply_ods_airports_ddl
        >> apply_ods_airplanes_ddl
        >> apply_ods_routes_ddl
        >> apply_ods_seats_ddl
        >> apply_ods_bookings_ddl
        >> apply_ods_tickets_ddl
        >> apply_ods_flights_ddl
        >> apply_ods_segments_ddl
        >> apply_ods_boarding_passes_ddl
    )
