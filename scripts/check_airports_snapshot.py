"""Независимая сверка лабораторной PXF: Postgres → сохраненный файл → STG/ODS."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ("airport_code", "airport_name", "city", "country", "coordinates", "timezone")
JSON_FIELDS = {"airport_name", "city", "country"}


def read_rows(service: str, query: str) -> list[dict]:
    """Читает через psql в контейнере; настройки берет из окружения Compose."""
    if service == "bookings-db":
        command = (
            'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -X -qAt '
            '-v ON_ERROR_STOP=1 -h 127.0.0.1 -U "$POSTGRES_USER" -d demo'
        )
    else:
        command = (
            'PGPASSWORD="$GREENPLUM_PASSWORD" exec '
            "/usr/local/greenplum-db/bin/psql -X -qAt -v ON_ERROR_STOP=1 "
            '-h 127.0.0.1 -U "$GREENPLUM_USER" -d "$GREENPLUM_DATABASE_NAME"'
        )
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", service, "sh", "-c", command],
        input=f"SELECT row_to_json(r) FROM ({query}) r;\n",
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def compare_snapshot(
    expected: list[dict], actual: list[dict], batch_id: str, layer: str
) -> list[str]:
    """Проверяет ключи, все бизнес-поля и метки; порядок строк не важен."""
    errors = []
    if not expected:
        return ["Контрольный источник пуст. Сначала восстановите Bookings."]
    source = {row["airport_code"]: row for row in expected}
    if len(source) != len(expected) or None in source or "" in source:
        return ["В контрольном источнике пустые или повторяющиеся ключи."]
    keys = [row["airport_code"] for row in actual]
    if len(keys) != len(set(keys)):
        errors.append("Повторяются airport_code внутри снимка.")
    missing = source.keys() - set(keys)
    extra = set(keys) - source.keys()
    if missing:
        errors.append(
            f"Не получены ключи: {sorted(missing)[:5]} (всего {len(missing)})."
        )
    if extra:
        errors.append(f"Лишние ключи: {sorted(map(str, extra))[:5]}.")
    for row in actual:
        key = row["airport_code"]
        if row.get("_load_id") != batch_id:
            errors.append(
                f"{key}: _load_id не равен {batch_id}; проверьте выбор батча."
            )
        timestamps = ("_load_ts", "event_ts") if layer == "stg" else ("_load_ts",)
        if any(not row.get(field) for field in timestamps):
            errors.append(f"{key}: не заполнены метки времени {timestamps}.")
        if key not in source:
            continue
        for field in FIELDS:
            wanted, received = source[key][field], row[field]
            if field in JSON_FIELDS:
                # Сравниваем весь JSON в STG, включая языки, кроме русского.
                wanted = json.loads(wanted) if wanted is not None else None
                if layer == "ods":
                    wanted = wanted.get("ru") if wanted is not None else None
                elif received is not None:
                    try:
                        received = json.loads(received)
                    except (ValueError, TypeError):
                        errors.append(f"{key}: {field} не содержит исходный JSON.")
                        continue
            if received != wanted:
                errors.append(f"{key}: отличается {field}.")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    export = actions.add_parser(
        "export", help="Сохранить источник напрямую из Postgres"
    )
    export.add_argument(
        "file", type=Path, help="Новый JSON-файл для контрольного источника"
    )
    check = actions.add_parser("check", help="Сверить снимок с сохраненным источником")
    check.add_argument("batch_id", help="Run ID нужного запуска STG")
    check.add_argument("file", type=Path, help="Контрольный JSON-файл из export")
    check.add_argument("--layer", choices=("stg", "ods"), default="stg")
    args = parser.parse_args()

    try:
        if args.action == "export":
            fields = ", ".join(f"{field}::text AS {field}" for field in FIELDS)
            rows = read_rows(
                "bookings-db",
                f"SELECT {fields} FROM bookings.airports_data ORDER BY airport_code",
            )
            if not rows:
                raise ValueError(
                    "Источник пуст. Пройдите инициализацию Bookings из README."
                )
            rows.sort(key=lambda row: row["airport_code"])
            # Не даем случайно заменить контроль A данными измененного источника B.
            with args.file.open("x", encoding="utf-8") as output:
                json.dump(rows, output, ensure_ascii=False, indent=2)
                output.write("\n")
            print(f"Источник: {len(rows)} аэропортов сохранено в {args.file}.")
            return 0

        expected = json.loads(args.file.read_text(encoding="utf-8"))
        # Run ID передается SQL-литералом, а не частью команды shell.
        batch_literal = "'" + args.batch_id.replace("'", "''") + "'"
        query = f"SELECT * FROM {args.layer}.airports"
        if args.layer == "stg":
            query += f" WHERE _load_id = {batch_literal}"
        actual = read_rows("greenplum", query)
        errors = compare_snapshot(expected, actual, args.batch_id, args.layer)
        if errors:
            print("Сверка не прошла. Проверьте SQL загрузки и выбранный Run ID:")
            for error in errors[:10]:
                print(f"- {error}")
            return 1
        print(
            f"Сверка пройдена: {args.layer}.airports, _load_id={args.batch_id}, "
            f"{len(actual)} ключей; все бизнес-поля и метки совпадают с требованиями."
        )
        return 0
    except subprocess.CalledProcessError as error:
        print("Не удалось прочитать БД. Проверьте docker compose ps и настройки .env.")
        print(error.stderr.strip())
        return 1
    except FileExistsError:
        print(
            "Контрольный файл уже существует. Сохраните его; для нового опыта выберите другой путь."
        )
        return 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(
            f"Не удалось выполнить сверку: {error}. Проверьте путь и формат контрольного файла."
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
