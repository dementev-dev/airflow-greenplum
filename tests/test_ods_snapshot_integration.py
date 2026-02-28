from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_ODS_INTEGRATION = os.getenv("RUN_ODS_INTEGRATION") == "1"
BATCH_TOKEN = '{{ ti.xcom_pull(task_ids="resolve_stg_batch_id") }}'

pytestmark = pytest.mark.skipif(
    not RUN_ODS_INTEGRATION,
    reason="Set RUN_ODS_INTEGRATION=1 to run ODS integration tests",
)


def _psql(sql: str) -> str:
    """Выполняет SQL в Greenplum-контейнере и возвращает stdout psql."""
    docker_bin = os.getenv("DOCKER_BIN", "docker")
    cmd = [
        docker_bin,
        "compose",
        "-f",
        "docker-compose.yml",
        "exec",
        "-T",
        "greenplum",
        "bash",
        "-lc",
        "su - gpadmin -c '/usr/local/greenplum-db/bin/psql -v ON_ERROR_STOP=1 -d gp_dwh -At -f -'",
    ]
    result = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        input=sql,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def _render_airports_sql(
    path: str, batch_id: str, stg_table: str, ods_table: str
) -> str:
    sql = (PROJECT_ROOT / path).read_text(encoding="utf-8")
    sql = sql.replace(BATCH_TOKEN, batch_id)
    sql = sql.replace("stg.airports", stg_table)
    sql = sql.replace("ods.airports", ods_table)
    return sql


def test_snapshot_airports_contract_upsert_delete_and_dq() -> None:
    """
    Интеграционный тест контракта snapshot-таблицы:
    - UPSERT обновляет и вставляет;
    - DELETE синхронизирует current state по выбранному батчу;
    - DQ проходит на корректном состоянии.
    """
    stg_table = "public.it_stg_airports_ods"
    ods_table = "public.it_ods_airports_ods"

    setup_sql = f"""
    DROP TABLE IF EXISTS {stg_table};
    DROP TABLE IF EXISTS {ods_table};

    CREATE TABLE {stg_table} (
        airport_code      TEXT,
        airport_name      TEXT,
        city              TEXT,
        country           TEXT,
        coordinates       TEXT,
        timezone          TEXT,
        src_created_at_ts TIMESTAMP,
        load_dttm         TIMESTAMP,
        batch_id          TEXT
    );

    CREATE TABLE {ods_table} (
        airport_code TEXT NOT NULL,
        airport_name TEXT NOT NULL,
        city         TEXT NOT NULL,
        country      TEXT NOT NULL,
        coordinates  TEXT,
        timezone     TEXT NOT NULL,
        _load_id     TEXT NOT NULL,
        _load_ts     TIMESTAMP NOT NULL DEFAULT now()
    );
    """
    _psql(setup_sql)

    try:
        _psql(
            f"""
            INSERT INTO {stg_table} (
                airport_code, airport_name, city, country, coordinates, timezone,
                src_created_at_ts, load_dttm, batch_id
            ) VALUES
                ('AAA', '{{"en": "Airport A", "ru": "Аэропорт A"}}', '{{"en": "City A", "ru": "Город A"}}', '{{"en": "Country A", "ru": "Страна A"}}', '(0,0)', 'UTC', now(), now(), 'batch_1'),
                ('BBB', '{{"en": "Airport B", "ru": "Аэропорт B"}}', '{{"en": "City B", "ru": "Город B"}}', '{{"en": "Country B", "ru": "Страна B"}}', '(1,1)', 'UTC', now(), now(), 'batch_1');
            """
        )

        _psql(
            _render_airports_sql(
                "sql/ods/airports_load.sql", "batch_1", stg_table, ods_table
            )
        )

        codes_batch_1 = _psql(
            f"SELECT COALESCE(string_agg(airport_code, ',' ORDER BY airport_code), '') FROM {ods_table};"
        ).strip()
        assert codes_batch_1 == "AAA,BBB"

        _psql(
            f"""
            INSERT INTO {stg_table} (
                airport_code, airport_name, city, country, coordinates, timezone,
                src_created_at_ts, load_dttm, batch_id
            ) VALUES
                ('AAA', '{{"en": "Airport A v2", "ru": "Аэропорт A v2"}}', '{{"en": "City A", "ru": "Город A"}}', '{{"en": "Country A", "ru": "Страна A"}}', '(0,0)', 'UTC', now(), now(), 'batch_2'),
                ('CCC', '{{"en": "Airport C", "ru": "Аэропорт C"}}', '{{"en": "City C", "ru": "Город C"}}', '{{"en": "Country C", "ru": "Страна C"}}', '(2,2)', 'UTC', now(), now(), 'batch_2');
            """
        )

        _psql(
            _render_airports_sql(
                "sql/ods/airports_load.sql", "batch_2", stg_table, ods_table
            )
        )

        codes_batch_2 = _psql(
            f"SELECT COALESCE(string_agg(airport_code, ',' ORDER BY airport_code), '') FROM {ods_table};"
        ).strip()
        assert codes_batch_2 == "AAA,CCC"

        # ODS теперь содержит русские названия (извлечены из JSON)
        airport_a_name = _psql(
            f"SELECT airport_name FROM {ods_table} WHERE airport_code = 'AAA';"
        ).strip()
        assert airport_a_name == "Аэропорт A v2"

        _psql(
            _render_airports_sql(
                "sql/ods/airports_dq.sql", "batch_2", stg_table, ods_table
            )
        )
    finally:
        _psql(f"DROP TABLE IF EXISTS {stg_table}; DROP TABLE IF EXISTS {ods_table};")
