"""Проверка должна отвергать ошибки, которые не видны по количеству строк."""

import importlib.util
from copy import deepcopy
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "check_airports_snapshot",
    Path(__file__).resolve().parents[1] / "scripts/check_airports_snapshot.py",
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

SOURCE = [
    {
        "airport_code": "AAA",
        "airport_name": '{"en": "Airport", "ru": "Аэропорт"}',
        "city": '{"ru": "Город"}',
        "country": '{"ru": "Страна"}',
        "coordinates": "(1,2)",
        "timezone": "Europe/Moscow",
    }
]


def snapshot():
    """Снимок с одним ключом и обязательными метками загрузки."""
    rows = deepcopy(SOURCE)
    rows[0].update(
        _load_id="B", _load_ts="2026-09-22 12:00:00", event_ts="2026-09-22 12:00:00"
    )
    return rows


def test_accepts_complete_snapshot_with_json_key_order_difference():
    rows = snapshot()
    rows[0]["airport_name"] = '{"ru":"Аэропорт","en":"Airport"}'
    assert checker.compare_snapshot(SOURCE, rows, "B", "stg") == []


def test_accepts_different_row_order_from_segments():
    """Порядок выдачи строк Greenplum не меняет содержимое снимка."""
    expected = deepcopy(SOURCE) + deepcopy(SOURCE)
    expected[1]["airport_code"] = "BBB"
    rows = snapshot() + snapshot()
    rows[1]["airport_code"] = "BBB"
    assert checker.compare_snapshot(expected, rows[::-1], "B", "stg") == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("airport_code", "BBB"),
        ("airport_name", '{"ru":"Старое имя","en":"Airport"}'),
        ("country", '{"ru":"Другая страна"}'),
        ("coordinates", "(2,1)"),
        ("_load_id", "A"),
        ("event_ts", None),
    ],
)
def test_rejects_wrong_values_despite_equal_row_count(field, value):
    rows = snapshot()
    rows[0][field] = value
    assert checker.compare_snapshot(SOURCE, rows, "B", "stg")


def test_rejects_duplicate_and_empty_snapshot():
    rows = snapshot()
    assert checker.compare_snapshot(SOURCE, rows + rows, "B", "stg")
    assert checker.compare_snapshot(SOURCE, [], "B", "stg")


def test_ods_requires_both_transformation_and_selected_batch():
    rows = snapshot()
    rows[0].update(airport_name="Аэропорт", city="Город", country="Страна")
    assert checker.compare_snapshot(SOURCE, rows, "B", "ods") == []
    rows[0]["_load_id"] = "A"
    assert checker.compare_snapshot(SOURCE, rows, "B", "ods")
