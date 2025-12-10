from __future__ import annotations

"""
Учебный DAG: создаёт схему stg и таблицы bookings_ext/bookings в Greenplum.
Запускается вручную перед DAG загрузки bookings_to_gp_stage или после изменения DDL.
"""

from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="bookings_stg_ddl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "greenplum", "ddl", "bookings", "stg"],
    description="Создаёт/обновляет stg.bookings_ext и stg.bookings для учебного DAG",
) as dag:
    apply_stg_bookings_ddl = PostgresOperator(
        task_id="apply_stg_bookings_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="/sql/stg/bookings_ddl.sql",
    )
