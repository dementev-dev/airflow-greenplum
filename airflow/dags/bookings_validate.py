"""
Валидационный DAG: самопроверка студенческих заданий.

Запускается вручную в Airflow UI после реализации заданий.
Таски сгруппированы по слоям — студент видит, где именно проблема.

Структура:
  validate_ods  — проверки ODS: rowcount, дубли BK, NULL в PK
  validate_dds  — проверки DDS: SCD1-измерения, SCD2 dim_routes (активный тест!)
  validate_dm   — проверки DM-витрин: не пусты, нет NULL в ключах

Все три группы запускаются параллельно — инкрементальная обратная связь:
студент может проверить только ODS, пока DDS/DM ещё не реализованы.
"""

from datetime import timedelta
from logging import getLogger

import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"
log = getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 0,  # Без ретраев — студент должен увидеть ошибку сразу
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="bookings_validate",
    start_date=pendulum.datetime(2017, 1, 1, tz="UTC"),
    schedule=None,  # Только ручной запуск
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "validate"],
    description="Валидация студенческих заданий: ODS, DDS, DM",
) as dag:

    with TaskGroup("validate_ods") as validate_ods:
        check_ods_airplanes = PostgresOperator(
            task_id="check_ods_airplanes_rowcount",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_airplanes_rowcount.sql",
        )
        check_ods_seats = PostgresOperator(
            task_id="check_ods_seats_rowcount",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_seats_rowcount.sql",
        )
        check_ods_dup_bk = PostgresOperator(
            task_id="check_ods_no_dup_bk",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_no_dup_bk.sql",
        )
        check_ods_pks = PostgresOperator(
            task_id="check_ods_no_null_pks",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/ods_no_null_pks.sql",
        )

    with TaskGroup("validate_dds") as validate_dds:
        check_dim_airplanes = PostgresOperator(
            task_id="check_dim_airplanes_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_airplanes_exists.sql",
        )
        check_dim_passengers = PostgresOperator(
            task_id="check_dim_passengers_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_passengers_exists.sql",
        )
        check_dim_passengers_dup = PostgresOperator(
            task_id="check_dim_passengers_no_dup_bk",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_passengers_no_dup_bk.sql",
        )
        check_dim_routes = PostgresOperator(
            task_id="check_dim_routes_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_exists.sql",
        )

        # SCD2 активный тест: цепочка backup → mutate → load → check → restore
        scd2_backup = PostgresOperator(
            task_id="scd2_backup",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_backup.sql",
        )
        scd2_mutate = PostgresOperator(
            task_id="scd2_mutate",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_mutate.sql",
        )
        scd2_run_load = PostgresOperator(
            task_id="scd2_run_student_load",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="dds/dim_routes_load.sql",  # студенческий load-скрипт!
        )
        scd2_check = PostgresOperator(
            task_id="scd2_check",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_check.sql",
        )
        scd2_restore = PostgresOperator(
            task_id="scd2_restore",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_scd2_restore.sql",
            trigger_rule="all_done",  # Откат ВСЕГДА, даже если check упал
        )

        check_dim_routes_gaps = PostgresOperator(
            task_id="check_dim_routes_no_gaps",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/dim_routes_no_gaps.sql",
        )

        # SCD2 цепочка
        scd2_backup >> scd2_mutate >> scd2_run_load >> scd2_check >> scd2_restore

        # no_gaps запускается после restore (на чистых данных)
        scd2_restore >> check_dim_routes_gaps

    with TaskGroup("validate_dm") as validate_dm:
        check_airport_traffic = PostgresOperator(
            task_id="check_airport_traffic_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/airport_traffic_exists.sql",
        )
        check_route_performance = PostgresOperator(
            task_id="check_route_performance_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/route_performance_exists.sql",
        )
        check_monthly_overview = PostgresOperator(
            task_id="check_monthly_overview_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/monthly_overview_exists.sql",
        )
        check_passenger_loyalty = PostgresOperator(
            task_id="check_passenger_loyalty_exists",
            postgres_conn_id=GREENPLUM_CONN_ID,
            sql="validate/passenger_loyalty_exists.sql",
        )
