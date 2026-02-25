from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_ENTITIES = ("airports", "airplanes", "routes", "seats")


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def test_snapshot_load_scripts_sync_deleted_keys() -> None:
    """Snapshot-таблицы в ODS должны удалять ключи, отсутствующие в текущем батче."""
    for entity in SNAPSHOT_ENTITIES:
        sql = _read(f"sql/ods/{entity}_load.sql")
        assert f"DELETE FROM ods.{entity} AS o" in sql
        assert "WITH src_keys AS (" in sql
        assert "WHERE NOT EXISTS (" in sql


def test_snapshot_dq_checks_extra_keys() -> None:
    """DQ snapshot-таблиц должен ловить лишние ключи в ODS относительно текущего батча STG."""
    for entity in SNAPSHOT_ENTITIES:
        sql = _read(f"sql/ods/{entity}_dq.sql")
        assert "v_extra_keys_count" in sql
        assert "найдены лишние ключи" in sql


def test_ods_batch_resolver_uses_consistent_snapshot_batches() -> None:
    """Резолвер батча должен искать batch_id, общий для всех snapshot-таблиц STG."""
    dag_code = _read("airflow/dags/bookings_to_gp_ods.py")

    for table_name in ("stg.airports", "stg.airplanes", "stg.routes", "stg.seats"):
        assert table_name in dag_code

    assert "INTERSECT" in dag_code
    assert "candidate_batches" in dag_code


def test_flights_load_covers_segment_flight_ids_from_stg_history() -> None:
    """
    Загрузка flights должна подтягивать рейсы из истории STG,
    если на них ссылаются segments текущего батча.
    """
    sql = _read("sql/ods/flights_load.sql")

    assert "segment_flights" in sql
    assert "FROM stg.segments AS s" in sql
    assert "JOIN segment_flights AS sf" in sql
