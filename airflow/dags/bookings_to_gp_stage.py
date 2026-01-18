from __future__ import annotations

"""
Учебный DAG для менти: показывает, как устроен поток
от источника bookings-db до слоя stg в Greenplum.

В этом примере мы сознательно используем:
- Airflow Connections для подключения к БД;
- PostgresOperator, который берёт SQL-скрипты с диска;
чтобы студент увидел «канонический» способ работы с SQL в DAG.

Каждый запуск DAG работает как «шаг по времени вперёд»:
- генератор в демо-БД bookings добавляет следующий учебный день после max(book_date);
- загрузка в Greenplum берёт все строки, появившиеся после предыдущих батчей;
- `run_id` используется как метка запуска (в `batch_id`, в логах и DQ).

Важно: для инкрементальных таблиц «пустое окно инкремента» допустимо (это не ошибка).
"""

from datetime import timedelta

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow import DAG

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


BOOKINGS_CONN_ID = "bookings_db"
GREENPLUM_CONN_ID = "greenplum_conn"


def _finish_summary() -> None:
    """
    Логирует краткий итог выполнения DAG за один запуск.

    Здесь можно добавить дополнительную агрегацию/логирование,
    но для учебного примера достаточно простого сообщения.
    """
    from logging import getLogger

    log = getLogger(__name__)
    log.info("DAG bookings_to_gp_stage завершён. Подробности смотрите в логах задач.")


with DAG(
    dag_id="bookings_to_gp_stage",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="/sql",
    default_args=default_args,
    tags=["demo", "bookings", "greenplum", "stg"],
    description="Учебный DAG: загрузка из bookings-db в слой stg (Greenplum) + DQ проверки",
) as dag:
    # 1. Генерируем один (или несколько стартовых) учебный день в демо-БД bookings
    generate_bookings_day = PostgresOperator(
        task_id="generate_bookings_day",
        postgres_conn_id=BOOKINGS_CONN_ID,
        sql="src/bookings_generate_day_if_missing.sql",
        autocommit=True,
    )

    # 2. Загружаем инкремент из stg.bookings_ext в stg.bookings
    load_bookings_to_stg = PostgresOperator(
        task_id="load_bookings_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/bookings_load.sql",
    )

    # 3. Проверяем количество строк между источником и stg.bookings
    check_row_counts = PostgresOperator(
        task_id="check_row_counts",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/bookings_dq.sql",
    )

    # 4. Загружаем инкремент билетов из stg.tickets_ext в stg.tickets
    load_tickets_to_stg = PostgresOperator(
        task_id="load_tickets_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/tickets_load.sql",
    )

    # 5. Проверяем качество данных для tickets
    check_tickets_dq = PostgresOperator(
        task_id="check_tickets_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/tickets_dq.sql",
    )

    # Загрузка справочников (full load)
    load_airports_to_stg = PostgresOperator(
        task_id="load_airports_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/airports_load.sql",
    )

    check_airports_dq = PostgresOperator(
        task_id="check_airports_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/airports_dq.sql",
    )

    load_airplanes_to_stg = PostgresOperator(
        task_id="load_airplanes_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/airplanes_load.sql",
    )

    check_airplanes_dq = PostgresOperator(
        task_id="check_airplanes_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/airplanes_dq.sql",
    )

    load_routes_to_stg = PostgresOperator(
        task_id="load_routes_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/routes_load.sql",
    )

    check_routes_dq = PostgresOperator(
        task_id="check_routes_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/routes_dq.sql",
    )

    load_seats_to_stg = PostgresOperator(
        task_id="load_seats_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/seats_load.sql",
    )

    check_seats_dq = PostgresOperator(
        task_id="check_seats_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/seats_dq.sql",
    )

    # Загрузка транзакций (инкремент/full snapshot)
    load_flights_to_stg = PostgresOperator(
        task_id="load_flights_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/flights_load.sql",
    )

    check_flights_dq = PostgresOperator(
        task_id="check_flights_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/flights_dq.sql",
    )

    load_segments_to_stg = PostgresOperator(
        task_id="load_segments_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/segments_load.sql",
    )

    check_segments_dq = PostgresOperator(
        task_id="check_segments_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/segments_dq.sql",
    )

    load_boarding_passes_to_stg = PostgresOperator(
        task_id="load_boarding_passes_to_stg",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/boarding_passes_load.sql",
    )

    check_boarding_passes_dq = PostgresOperator(
        task_id="check_boarding_passes_dq",
        postgres_conn_id=GREENPLUM_CONN_ID,
        sql="stg/boarding_passes_dq.sql",
    )

    # Финальный лог/сводка
    finish_summary = PythonOperator(
        task_id="finish_summary",
        python_callable=_finish_summary,
    )

    # Сначала загружаются и проверяются bookings и tickets
    generate_bookings_day >> load_bookings_to_stg >> check_row_counts
    check_row_counts >> load_tickets_to_stg >> check_tickets_dq

    # Затем загружаются справочники.
    # Для простоты (и более понятных логов для новичков) делаем это последовательно.
    # Если позже понадобится ускорить DAG, эти шаги можно распараллелить, сохранив зависимости.
    check_tickets_dq >> load_airports_to_stg >> check_airports_dq
    check_airports_dq >> load_airplanes_to_stg >> check_airplanes_dq
    check_airplanes_dq >> load_routes_to_stg >> check_routes_dq
    check_routes_dq >> load_seats_to_stg >> check_seats_dq

    # Затем загружаются транзакции (тоже последовательно, по тем же причинам).
    check_seats_dq >> load_flights_to_stg >> check_flights_dq
    check_flights_dq >> load_segments_to_stg >> check_segments_dq
    check_segments_dq >> load_boarding_passes_to_stg >> check_boarding_passes_dq

    # В конце финальный лог
    check_boarding_passes_dq >> finish_summary
