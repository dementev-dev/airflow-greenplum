from __future__ import annotations

"""
Учебный DAG: применяет DDL для базовой таблицы orders в Greenplum.
Запускается вручную перед CSV‑пайплайном или после изменения схемы.
"""

from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="orders_base_ddl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "greenplum", "ddl", "orders"],
    description="Создаёт/обновляет базовую таблицу orders в схеме public",
) as dag:
    apply_orders_ddl = PostgresOperator(
        task_id="apply_orders_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="base/orders_ddl.sql",
    )
