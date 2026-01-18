from __future__ import annotations

import importlib

import pytest


def _airflow_available() -> bool:
    try:
        af = importlib.import_module("airflow")
    except Exception:
        return False
    # Real Airflow exposes DAG at top-level
    return hasattr(af, "DAG")


pytestmark = pytest.mark.skipif(
    not _airflow_available(), reason="Airflow is not installed for DAG smoke tests"
)


def _load_dag(module_name: str):
    mod = importlib.import_module(module_name)
    assert hasattr(mod, "dag"), f"{module_name} must expose variable 'dag'"
    return getattr(mod, "dag")


def test_csv_to_greenplum_dag_structure():
    dag = _load_dag("airflow.dags.csv_to_greenplum")

    # tasks
    expected_tasks = {
        "create_orders_table",
        "generate_csv",
        "preview_csv",
        "load_csv_to_greenplum",
    }
    assert expected_tasks.issubset(dag.task_dict.keys())

    # linear dependencies
    t1 = dag.get_task("create_orders_table")
    t2 = dag.get_task("generate_csv")
    t3 = dag.get_task("preview_csv")
    t4 = dag.get_task("load_csv_to_greenplum")

    assert t2 in t1.get_direct_relatives("downstream")
    assert t3 in t2.get_direct_relatives("downstream")
    assert t4 in t3.get_direct_relatives("downstream")


def test_csv_to_greenplum_dq_dag_structure():
    dag = _load_dag("airflow.dags.csv_to_greenplum_dq")

    expected_tasks = {
        "check_orders_table_exists",
        "check_orders_schema",
        "check_orders_has_rows",
        "check_order_duplicates",
        "data_quality_summary",
    }
    assert expected_tasks.issubset(dag.task_dict.keys())

    e = dag.get_task("check_orders_table_exists")
    s = dag.get_task("check_orders_schema")
    h = dag.get_task("check_orders_has_rows")
    d = dag.get_task("check_order_duplicates")
    q = dag.get_task("data_quality_summary")

    assert s in e.get_direct_relatives("downstream")
    assert h in s.get_direct_relatives("downstream")
    assert d in h.get_direct_relatives("downstream")
    assert q in d.get_direct_relatives("downstream")


def test_bookings_stg_ddl_dag_structure():
    """Проверка структуры DAG bookings_stg_ddl."""
    dag = _load_dag("airflow.dags.bookings_stg_ddl")

    expected_tasks = {
        "apply_stg_bookings_ddl",
        "apply_stg_tickets_ddl",
        "apply_stg_airports_ddl",
        "apply_stg_airplanes_ddl",
        "apply_stg_routes_ddl",
        "apply_stg_seats_ddl",
        "apply_stg_flights_ddl",
        "apply_stg_segments_ddl",
        "apply_stg_boarding_passes_ddl",
    }
    assert expected_tasks.issubset(dag.task_dict.keys())

    # Проверка линейных зависимостей
    # Справочники создаются после bookings/tickets
    assert dag.has_task("apply_stg_bookings_ddl")
    assert dag.has_task("apply_stg_tickets_ddl")
    assert dag.has_task("apply_stg_airports_ddl")
    assert dag.has_task("apply_stg_airplanes_ddl")
    assert dag.has_task("apply_stg_routes_ddl")
    assert dag.has_task("apply_stg_seats_ddl")
    assert dag.has_task("apply_stg_flights_ddl")
    assert dag.has_task("apply_stg_segments_ddl")
    assert dag.has_task("apply_stg_boarding_passes_ddl")


def test_bookings_to_gp_stage_dag_structure():
    """Проверка структуры DAG bookings_to_gp_stage."""
    dag = _load_dag("airflow.dags.bookings_to_gp_stage")

    expected_tasks = {
        "generate_bookings_day",
        "load_bookings_to_stg",
        "check_row_counts",
        "load_tickets_to_stg",
        "check_tickets_dq",
        "load_airports_to_stg",
        "check_airports_dq",
        "load_airplanes_to_stg",
        "check_airplanes_dq",
        "load_routes_to_stg",
        "check_routes_dq",
        "load_seats_to_stg",
        "check_seats_dq",
        "load_flights_to_stg",
        "check_flights_dq",
        "load_segments_to_stg",
        "check_segments_dq",
        "load_boarding_passes_to_stg",
        "check_boarding_passes_dq",
        "finish_summary",
    }
    assert expected_tasks.issubset(dag.task_dict.keys())

    # Проверка линейных зависимостей
    # bookings/tickets → справочники → транзакции → финальный лог
    assert dag.has_task("generate_bookings_day")
    assert dag.has_task("load_bookings_to_stg")
    assert dag.has_task("check_row_counts")
    assert dag.has_task("load_tickets_to_stg")
    assert dag.has_task("check_tickets_dq")
    assert dag.has_task("load_airports_to_stg")
    assert dag.has_task("check_airports_dq")
    assert dag.has_task("load_airplanes_to_stg")
    assert dag.has_task("check_airplanes_dq")
    assert dag.has_task("load_routes_to_stg")
    assert dag.has_task("check_routes_dq")
    assert dag.has_task("load_seats_to_stg")
    assert dag.has_task("check_seats_dq")
    assert dag.has_task("load_flights_to_stg")
    assert dag.has_task("check_flights_dq")
    assert dag.has_task("load_segments_to_stg")
    assert dag.has_task("check_segments_dq")
    assert dag.has_task("load_boarding_passes_to_stg")
    assert dag.has_task("check_boarding_passes_dq")
    assert dag.has_task("finish_summary")
