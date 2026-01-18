from __future__ import annotations

"""
Учебный DAG: создаёт/обновляет слой stg в Greenplum для демо-источника bookings.

Запускается вручную перед DAG загрузки `bookings_to_gp_stage` или после изменения DDL.
Создаёт внешние таблицы PXF (`*_ext`) и внутренние таблицы STG (9 таблиц: bookings, tickets,
airports, airplanes, routes, seats, flights, segments, boarding_passes).
"""

from datetime import timedelta

import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="bookings_stg_ddl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "greenplum", "ddl", "bookings", "stg"],
    description="Учебный DDL DAG: создаёт/обновляет stg.* (PXF external + internal STG) для bookings",
) as dag:
    apply_stg_bookings_ddl = PostgresOperator(
        task_id="apply_stg_bookings_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/bookings_ddl.sql",
    )

    apply_stg_tickets_ddl = PostgresOperator(
        task_id="apply_stg_tickets_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/tickets_ddl.sql",
    )

    # DDL для справочников
    apply_stg_airports_ddl = PostgresOperator(
        task_id="apply_stg_airports_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/airports_ddl.sql",
    )

    apply_stg_airplanes_ddl = PostgresOperator(
        task_id="apply_stg_airplanes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/airplanes_ddl.sql",
    )

    apply_stg_routes_ddl = PostgresOperator(
        task_id="apply_stg_routes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/routes_ddl.sql",
    )

    apply_stg_seats_ddl = PostgresOperator(
        task_id="apply_stg_seats_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/seats_ddl.sql",
    )

    # DDL для транзакционных таблиц
    apply_stg_flights_ddl = PostgresOperator(
        task_id="apply_stg_flights_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/flights_ddl.sql",
    )

    apply_stg_segments_ddl = PostgresOperator(
        task_id="apply_stg_segments_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/segments_ddl.sql",
    )

    apply_stg_boarding_passes_ddl = PostgresOperator(
        task_id="apply_stg_boarding_passes_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/boarding_passes_ddl.sql",
    )

    # DDL применяем последовательно, чтобы порядок был понятным для новичков,
    # а ошибки — воспроизводимыми (в логах сразу видно, на каком объекте упали).
    (
        apply_stg_bookings_ddl
        >> apply_stg_tickets_ddl
        >> apply_stg_airports_ddl
        >> apply_stg_airplanes_ddl
        >> apply_stg_routes_ddl
        >> apply_stg_seats_ddl
        >> apply_stg_flights_ddl
        >> apply_stg_segments_ddl
        >> apply_stg_boarding_passes_ddl
    )
