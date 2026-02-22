from __future__ import annotations

"""
Учебный DAG: загрузка из STG в ODS (Greenplum) по домену bookings.

Ключевая идея:
- весь запуск ODS работает с одним stg_batch_id;
- для каждой сущности выполняем пару задач load -> dq;
- загрузка реализована как SCD1 UPSERT (UPDATE изменившихся + INSERT новых).
"""

from datetime import timedelta
from logging import getLogger

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

GREENPLUM_CONN_ID = "greenplum_conn"

log = getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _resolve_stg_batch_id(**context) -> str:
    """
    Возвращает stg_batch_id из dag_run.conf или вычисляет последний согласованный батч.

    Согласованным считаем batch_id, который есть во всех snapshot-справочниках STG:
    airports, airplanes, routes, seats.
    """
    conf = context["dag_run"].conf or {}
    stg_batch_id = conf.get("stg_batch_id")

    if not stg_batch_id:
        hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        result = hook.get_first(
            """
            WITH candidate_batches AS (
                SELECT batch_id
                FROM stg.airports
                WHERE batch_id IS NOT NULL AND batch_id <> ''
                GROUP BY batch_id
                INTERSECT
                SELECT batch_id
                FROM stg.airplanes
                WHERE batch_id IS NOT NULL AND batch_id <> ''
                GROUP BY batch_id
                INTERSECT
                SELECT batch_id
                FROM stg.routes
                WHERE batch_id IS NOT NULL AND batch_id <> ''
                GROUP BY batch_id
                INTERSECT
                SELECT batch_id
                FROM stg.seats
                WHERE batch_id IS NOT NULL AND batch_id <> ''
                GROUP BY batch_id
            ),
            batch_ready AS (
                SELECT
                    c.batch_id,
                    GREATEST(
                        COALESCE(
                            (SELECT MAX(load_dttm) FROM stg.airports a WHERE a.batch_id = c.batch_id),
                            TIMESTAMP '1900-01-01 00:00:00'
                        ),
                        COALESCE(
                            (SELECT MAX(load_dttm) FROM stg.airplanes a WHERE a.batch_id = c.batch_id),
                            TIMESTAMP '1900-01-01 00:00:00'
                        ),
                        COALESCE(
                            (SELECT MAX(load_dttm) FROM stg.routes r WHERE r.batch_id = c.batch_id),
                            TIMESTAMP '1900-01-01 00:00:00'
                        ),
                        COALESCE(
                            (SELECT MAX(load_dttm) FROM stg.seats s WHERE s.batch_id = c.batch_id),
                            TIMESTAMP '1900-01-01 00:00:00'
                        )
                    ) AS ready_dttm
                FROM candidate_batches c
            )
            SELECT batch_id
            FROM batch_ready
            ORDER BY ready_dttm DESC
            LIMIT 1
            """
        )
        stg_batch_id = result[0] if result and result[0] else None

    if not stg_batch_id:
        raise ValueError(
            "stg_batch_id не найден: передайте stg_batch_id в conf или "
            "сначала выполните bookings_to_gp_stage для snapshot-справочников"
        )

    log.info("Используем stg_batch_id=%s", stg_batch_id)
    return stg_batch_id


def _finish_summary() -> None:
    """Логирует краткий итог выполнения ODS-ветки."""
    log.info("DAG bookings_to_gp_ods завершён. Подробности смотрите в логах задач.")


with DAG(
    dag_id="bookings_to_gp_ods",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "ods"],
    description="Учебный DAG: загрузка STG -> ODS (SCD1 UPSERT) + DQ проверки",
) as dag:
    resolve_stg_batch_id = PythonOperator(
        task_id="resolve_stg_batch_id",
        python_callable=_resolve_stg_batch_id,
    )

    load_ods_bookings = PostgresOperator(
        task_id="load_ods_bookings",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/bookings_load.sql",
    )

    dq_ods_bookings = PostgresOperator(
        task_id="dq_ods_bookings",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/bookings_dq.sql",
    )

    load_ods_tickets = PostgresOperator(
        task_id="load_ods_tickets",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/tickets_load.sql",
    )

    dq_ods_tickets = PostgresOperator(
        task_id="dq_ods_tickets",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/tickets_dq.sql",
    )

    load_ods_airports = PostgresOperator(
        task_id="load_ods_airports",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/airports_load.sql",
    )

    dq_ods_airports = PostgresOperator(
        task_id="dq_ods_airports",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/airports_dq.sql",
    )

    load_ods_airplanes = PostgresOperator(
        task_id="load_ods_airplanes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/airplanes_load.sql",
    )

    dq_ods_airplanes = PostgresOperator(
        task_id="dq_ods_airplanes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/airplanes_dq.sql",
    )

    load_ods_routes = PostgresOperator(
        task_id="load_ods_routes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/routes_load.sql",
    )

    dq_ods_routes = PostgresOperator(
        task_id="dq_ods_routes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/routes_dq.sql",
    )

    load_ods_seats = PostgresOperator(
        task_id="load_ods_seats",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/seats_load.sql",
    )

    dq_ods_seats = PostgresOperator(
        task_id="dq_ods_seats",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/seats_dq.sql",
    )

    load_ods_flights = PostgresOperator(
        task_id="load_ods_flights",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/flights_load.sql",
    )

    dq_ods_flights = PostgresOperator(
        task_id="dq_ods_flights",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/flights_dq.sql",
    )

    load_ods_segments = PostgresOperator(
        task_id="load_ods_segments",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/segments_load.sql",
    )

    dq_ods_segments = PostgresOperator(
        task_id="dq_ods_segments",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/segments_dq.sql",
    )

    load_ods_boarding_passes = PostgresOperator(
        task_id="load_ods_boarding_passes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/boarding_passes_load.sql",
    )

    dq_ods_boarding_passes = PostgresOperator(
        task_id="dq_ods_boarding_passes",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="ods/boarding_passes_dq.sql",
    )

    finish_ods_summary = PythonOperator(
        task_id="finish_ods_summary",
        python_callable=_finish_summary,
    )

    # stg_batch_id нужен всем загрузочным веткам.
    (
        resolve_stg_batch_id
        >> load_ods_bookings
        >> dq_ods_bookings
        >> load_ods_tickets
        >> dq_ods_tickets
    )
    resolve_stg_batch_id >> load_ods_airports >> dq_ods_airports
    resolve_stg_batch_id >> load_ods_airplanes >> dq_ods_airplanes

    [dq_ods_airports, dq_ods_airplanes] >> load_ods_routes >> dq_ods_routes
    dq_ods_airplanes >> load_ods_seats >> dq_ods_seats

    dq_ods_routes >> load_ods_flights >> dq_ods_flights
    [dq_ods_flights, dq_ods_tickets] >> load_ods_segments >> dq_ods_segments
    dq_ods_segments >> load_ods_boarding_passes >> dq_ods_boarding_passes

    [dq_ods_boarding_passes, dq_ods_seats] >> finish_ods_summary
