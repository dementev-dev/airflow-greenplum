from __future__ import annotations

"""
Учебный DAG: создаёт/обновляет DM-слой (Data Mart) в Greenplum для домена bookings.

Запускается вручную перед DAG загрузки `bookings_to_gp_dm` или после изменения DM DDL.
Создаёт 5 DM-витрин: sales_report, route_performance, passenger_loyalty,
airport_traffic, monthly_overview.

На данном этапе реализована только эталонная витрина sales_report.
Остальные витрины будут добавлены в последующих этапах.
"""

from datetime import timedelta

import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="bookings_dm_ddl",
    start_date=pendulum.datetime(2017, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "greenplum", "ddl", "bookings", "dm"],
    description="Учебный DDL DAG: создаёт/обновляет dm.* для bookings",
) as dag:
    # Эталонная витрина: sales_report
    apply_dm_sales_report_ddl = PostgresOperator(
        task_id="apply_dm_sales_report_ddl",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="dm/sales_report_ddl.sql",
    )

    # Заглушки для будущих витрин (будут реализованы в этапах 2-5)
    # apply_dm_route_performance_ddl = PostgresOperator(...)
    # apply_dm_passenger_loyalty_ddl = PostgresOperator(...)
    # apply_dm_airport_traffic_ddl = PostgresOperator(...)
    # apply_dm_monthly_overview_ddl = PostgresOperator(...)

    # Линейная цепочка (пока только одна витрина)
    # В будущем: sales_report >> route_performance >> passenger_loyalty
    #            >> airport_traffic >> monthly_overview
    apply_dm_sales_report_ddl
