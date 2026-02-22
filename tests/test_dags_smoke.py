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


def _assert_direct_edge(dag, upstream_task_id: str, downstream_task_id: str) -> None:
    upstream = dag.get_task(upstream_task_id)
    downstream = dag.get_task(downstream_task_id)
    assert downstream in upstream.get_direct_relatives(
        upstream=False
    ), f"Expected direct edge {upstream_task_id} -> {downstream_task_id}"


def _assert_reachable(dag, upstream_task_id: str, downstream_task_id: str) -> None:
    upstream = dag.get_task(upstream_task_id)
    downstream = dag.get_task(downstream_task_id)
    assert downstream in upstream.get_flat_relatives(
        upstream=False
    ), f"Expected {downstream_task_id} to be downstream of {upstream_task_id}"


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

    assert t2 in t1.get_direct_relatives(upstream=False)
    assert t3 in t2.get_direct_relatives(upstream=False)
    assert t4 in t3.get_direct_relatives(upstream=False)


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

    assert s in e.get_direct_relatives(upstream=False)
    assert h in s.get_direct_relatives(upstream=False)
    assert d in h.get_direct_relatives(upstream=False)
    assert q in d.get_direct_relatives(upstream=False)


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

    # Smoke-test графа: проверяем ключевые инварианты, не фиксируя линейный порядок.
    # Это позволяет в будущем распараллеливать независимые DDL-задачи.
    _assert_reachable(dag, "apply_stg_bookings_ddl", "apply_stg_tickets_ddl")

    for task_id in expected_tasks - {"apply_stg_bookings_ddl"}:
        _assert_reachable(dag, "apply_stg_bookings_ddl", task_id)


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

    # Smoke-test графа: проверяем инварианты, не фиксируя линейный порядок.
    # Это позволяет в будущем распараллеливать независимые загрузки справочников/транзакций.

    # Базовая цепочка должна сохраниться: генерация → bookings → DQ → tickets → DQ.
    _assert_reachable(dag, "generate_bookings_day", "load_bookings_to_stg")
    _assert_reachable(dag, "load_bookings_to_stg", "check_row_counts")
    _assert_reachable(dag, "check_row_counts", "load_tickets_to_stg")
    _assert_reachable(dag, "load_tickets_to_stg", "check_tickets_dq")

    # Инвариант "load → dq" для каждой таблицы.
    load_to_dq = [
        ("load_bookings_to_stg", "check_row_counts"),
        ("load_tickets_to_stg", "check_tickets_dq"),
        ("load_airports_to_stg", "check_airports_dq"),
        ("load_airplanes_to_stg", "check_airplanes_dq"),
        ("load_routes_to_stg", "check_routes_dq"),
        ("load_seats_to_stg", "check_seats_dq"),
        ("load_flights_to_stg", "check_flights_dq"),
        ("load_segments_to_stg", "check_segments_dq"),
        ("load_boarding_passes_to_stg", "check_boarding_passes_dq"),
    ]
    for load_task_id, dq_task_id in load_to_dq:
        _assert_direct_edge(dag, load_task_id, dq_task_id)

    # Барьеры по данным (не обязательно прямые рёбра).
    # routes_dq использует airports/airplanes текущего batch_id.
    _assert_reachable(dag, "check_airports_dq", "check_routes_dq")
    _assert_reachable(dag, "check_airplanes_dq", "check_routes_dq")

    # seats_dq использует airplanes текущего batch_id.
    _assert_reachable(dag, "check_airplanes_dq", "check_seats_dq")

    # flights_dq использует routes текущего batch_id.
    _assert_reachable(dag, "check_routes_dq", "check_flights_dq")

    # segments_dq проверяет наличие flights (STG-история); для первой загрузки flights должны быть до segments.
    _assert_reachable(dag, "check_flights_dq", "check_segments_dq")

    # boarding_passes_dq проверяет наличие segments/tickets (STG-история); для первой загрузки segments должны быть до DQ.
    _assert_reachable(dag, "check_segments_dq", "check_boarding_passes_dq")

    # Финальная сводка должна быть в конце графа (обе ветки).
    _assert_reachable(dag, "check_boarding_passes_dq", "finish_summary")
    _assert_reachable(dag, "check_seats_dq", "finish_summary")

    # Параллельность: airports и airplanes оба downstream от check_tickets_dq,
    # но НЕ зависят друг от друга (ни прямо, ни транзитивно).
    _assert_reachable(dag, "check_tickets_dq", "load_airports_to_stg")
    _assert_reachable(dag, "check_tickets_dq", "load_airplanes_to_stg")

    airports = dag.get_task("load_airports_to_stg")
    airplanes = dag.get_task("load_airplanes_to_stg")
    assert airplanes not in airports.get_flat_relatives(
        upstream=False
    ), "airports не должен быть upstream для airplanes"
    assert airports not in airplanes.get_flat_relatives(
        upstream=False
    ), "airplanes не должен быть upstream для airports"


def test_bookings_ods_ddl_dag_structure():
    """Проверка структуры DAG bookings_ods_ddl."""
    dag = _load_dag("airflow.dags.bookings_ods_ddl")

    expected_tasks = {
        "apply_ods_airports_ddl",
        "apply_ods_airplanes_ddl",
        "apply_ods_routes_ddl",
        "apply_ods_seats_ddl",
        "apply_ods_bookings_ddl",
        "apply_ods_tickets_ddl",
        "apply_ods_flights_ddl",
        "apply_ods_segments_ddl",
        "apply_ods_boarding_passes_ddl",
    }
    assert expected_tasks.issubset(dag.task_dict.keys())

    _assert_reachable(dag, "apply_ods_airports_ddl", "apply_ods_airplanes_ddl")

    for task_id in expected_tasks - {"apply_ods_airports_ddl"}:
        _assert_reachable(dag, "apply_ods_airports_ddl", task_id)


def test_bookings_to_gp_ods_dag_structure():
    """Проверка структуры DAG bookings_to_gp_ods."""
    dag = _load_dag("airflow.dags.bookings_to_gp_ods")

    expected_tasks = {
        "resolve_stg_batch_id",
        "load_ods_bookings",
        "dq_ods_bookings",
        "load_ods_tickets",
        "dq_ods_tickets",
        "load_ods_airports",
        "dq_ods_airports",
        "load_ods_airplanes",
        "dq_ods_airplanes",
        "load_ods_routes",
        "dq_ods_routes",
        "load_ods_seats",
        "dq_ods_seats",
        "load_ods_flights",
        "dq_ods_flights",
        "load_ods_segments",
        "dq_ods_segments",
        "load_ods_boarding_passes",
        "dq_ods_boarding_passes",
        "finish_ods_summary",
    }
    assert expected_tasks.issubset(dag.task_dict.keys())

    # Базовая цепочка транзакций.
    _assert_reachable(dag, "resolve_stg_batch_id", "load_ods_bookings")
    _assert_reachable(dag, "load_ods_bookings", "dq_ods_bookings")
    _assert_reachable(dag, "dq_ods_bookings", "load_ods_tickets")
    _assert_reachable(dag, "load_ods_tickets", "dq_ods_tickets")

    # Инвариант "load -> dq" для каждой таблицы.
    load_to_dq = [
        ("load_ods_bookings", "dq_ods_bookings"),
        ("load_ods_tickets", "dq_ods_tickets"),
        ("load_ods_airports", "dq_ods_airports"),
        ("load_ods_airplanes", "dq_ods_airplanes"),
        ("load_ods_routes", "dq_ods_routes"),
        ("load_ods_seats", "dq_ods_seats"),
        ("load_ods_flights", "dq_ods_flights"),
        ("load_ods_segments", "dq_ods_segments"),
        ("load_ods_boarding_passes", "dq_ods_boarding_passes"),
    ]
    for load_task_id, dq_task_id in load_to_dq:
        _assert_direct_edge(dag, load_task_id, dq_task_id)

    # Справочники стартуют параллельно и не зависят друг от друга.
    _assert_reachable(dag, "resolve_stg_batch_id", "load_ods_airports")
    _assert_reachable(dag, "resolve_stg_batch_id", "load_ods_airplanes")
    airports = dag.get_task("load_ods_airports")
    airplanes = dag.get_task("load_ods_airplanes")
    assert airplanes not in airports.get_flat_relatives(
        upstream=False
    ), "airports не должен быть upstream для airplanes"
    assert airports not in airplanes.get_flat_relatives(
        upstream=False
    ), "airplanes не должен быть upstream для airports"

    # Барьеры по данным.
    _assert_reachable(dag, "dq_ods_airports", "dq_ods_routes")
    _assert_reachable(dag, "dq_ods_airplanes", "dq_ods_routes")
    _assert_reachable(dag, "dq_ods_airplanes", "dq_ods_seats")
    _assert_reachable(dag, "dq_ods_routes", "dq_ods_flights")
    _assert_reachable(dag, "dq_ods_flights", "dq_ods_segments")
    _assert_reachable(dag, "dq_ods_tickets", "dq_ods_segments")
    _assert_reachable(dag, "dq_ods_segments", "dq_ods_boarding_passes")

    # Финальная сводка должна ждать обе ветки.
    _assert_reachable(dag, "dq_ods_boarding_passes", "finish_ods_summary")
    _assert_reachable(dag, "dq_ods_seats", "finish_ods_summary")
