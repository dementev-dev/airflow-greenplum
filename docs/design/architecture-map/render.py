# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

"""Проверить данные карты и собрать автономный HTML без внешних зависимостей."""

from __future__ import annotations

import argparse
import json
import re
from html import escape
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
OUTPUT = HERE.parent / "architecture-map.html"
MARKER = "/*__MAP_DATA__*/"
NOTICE = (
    "<!-- Собрано из architecture-map/map.json и template.html. "
    "Команда: uv run docs/design/architecture-map/render.py -->\n"
)


def headings(text: str) -> set[str]:
    """Якоря Markdown для ссылок на существующие разделы документации."""
    result = set(re.findall(r'<a\s+id="([^"]+)"', text))
    for title in re.findall(r"^#{1,6}\s+(.+)$", text, re.M):
        slug = re.sub(r"[^\w\- ]", "", title.lower()).replace(" ", "-")
        result.add(slug)
    return result


def validate(data: dict) -> None:
    """Сверить объекты, поля, статусы, входы готовых SQL и локальные ссылки."""
    repository = urlsplit(data["repository_url"])
    if (
        repository.scheme != "https"
        or not repository.netloc
        or not repository.path.endswith("/src/")
        or repository.query
        or repository.fragment
    ):
        raise ValueError("Укажите HTTPS-адрес репозитория Gitea с окончанием /src/")
    nodes = {node["id"]: node for node in data["nodes"]}
    if len(nodes) != len(data["nodes"]):
        raise ValueError("Повторяется идентификатор объекта")
    edge_ids = [e["id"] for e in data["edges"]]
    if len(set(edge_ids)) != len(edge_ids):
        raise ValueError("Повторяется идентификатор связи")
    columns, externals, tables = {}, {}, set()
    for path in sorted((ROOT / "sql").glob("*/*_ddl.sql")):
        text = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
        pattern = r"CREATE\s+(EXTERNAL\s+)?TABLE\s+(?:IF NOT EXISTS\s+)?(\w+\.\w+)\s*\((.*?)\n\)"
        for external, table, body in re.findall(pattern, text, re.S | re.I):
            columns[table] = set(re.findall(r"^\s*(\w+)\s+\w+", body, re.M))
            if external:
                source = re.search(r"pxf://([^?]+)", text)[1]
                columns[source] = columns[table]
                externals[table] = source
            else:
                tables.add(table)
    expected = tables | set(externals.values())
    if set(nodes) != expected:
        raise ValueError(f"Карта и DDL расходятся: {set(nodes) ^ expected}")
    parts = {part for n in nodes.values() for part in n["parts"]}
    if parts != set(externals):
        raise ValueError("В карточках STG должны быть представлены все внешние таблицы")
    layers = {layer["id"] for layer in data["layers"]}
    positions = set()
    for node in nodes.values():
        if node["layer"] not in layers:
            raise ValueError(f"Неизвестный слой: {node['id']}")
        position = (node["layer"], node["order"])
        if position in positions:
            raise ValueError(f"Объекты перекрываются: {position}")
        positions.add(position)
        for field in ("what", "grain", "keys", "fields", "fill", "links"):
            if not node[field]:
                raise ValueError(f"Пустое поле {field}: {node['id']}")
        for field in node["fields"]:
            if field["name"] not in columns[node["id"]]:
                raise ValueError(f"Нет поля {node['id']}.{field['name']} в DDL/PXF")
        if node["layer"] == "source":
            continue
        layer, name = node["id"].split(".")
        for role in ("load", "dq"):
            path = ROOT / f"sql/{layer}/{name}_{role}.sql"
            sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8")).strip()
            stub = sql.upper() == "SELECT 1;"
            if stub != (node["status"] == "student"):
                raise ValueError(
                    f"Статус карты не совпадает с {path.relative_to(ROOT)}"
                )
            if role != "load" or stub:
                continue
            sources = set(re.findall(r"\b(?:FROM|JOIN)\s+(\w+\.\w+)", sql, re.I))
            sources = {externals.get(s, s) for s in sources if s != node["id"]}
            sources &= expected
            drawn = {
                e["from"]
                for e in data["edges"]
                if e["mode"] == "flow" and e["to"] == node["id"]
            }
            if sources != drawn:
                raise ValueError(
                    f"Входы SQL и карты расходятся у {node['id']}: {sources ^ drawn}"
                )
    for edge in data["edges"]:
        if edge["from"] not in nodes or edge["to"] not in nodes:
            raise ValueError(f"Неизвестный конец связи {edge['id']}")
        if edge["mode"] not in ("flow", "keys") or not edge["join"] or not edge["refs"]:
            raise ValueError(f"Неполное описание связи {edge['id']}")
        for ref in edge["refs"]:
            if (
                ref["node"] not in nodes
                or not set(ref["columns"]) <= columns[ref["node"]]
            ):
                raise ValueError(f"Неизвестные поля связи {edge['id']}: {ref}")
    count = 0
    for item in [*nodes.values(), *data["edges"]]:
        for link in item["links"]:
            url = urlsplit(link["url"])
            if url.scheme or url.netloc:
                raise ValueError(f"Ссылка должна вести в локальный репозиторий: {link}")
            path = (OUTPUT.parent / unquote(url.path)).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file():
                raise ValueError(f"Файл по ссылке не найден: {link['url']}")
            if url.fragment and unquote(url.fragment) not in headings(
                path.read_text(encoding="utf-8")
            ):
                raise ValueError(f"Раздел по ссылке не найден: {link['url']}")
            count += 1
    print(
        f"Проверены {len(nodes)} объектов, {len(edge_ids)} связей, {count} ссылок; поля, статусы и входы SQL совпадают"
    )


def main() -> int:
    """Собрать HTML либо проверить его свежесть вместе с исходными данными."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="проверить без записи")
    args = parser.parse_args()
    try:
        data = json.loads((HERE / "map.json").read_text(encoding="utf-8"))
        validate(data)
        template = (HERE / "template.html").read_text(encoding="utf-8")
        if template.count(MARKER) != 1:
            raise ValueError("В шаблоне должен быть ровно один маркер данных")
        payload = json.dumps(data, ensure_ascii=False, indent=2).replace("<", "\\u003c")
        guide_url = urljoin(data["repository_url"], "docs/design/db_schema.md")
        result = NOTICE + template.replace(MARKER, payload).replace(
            "__GUIDE_URL__", escape(guide_url, quote=True)
        )
        if args.check:
            if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != result:
                raise ValueError(
                    "HTML устарел. Выполните uv run docs/design/architecture-map/render.py"
                )
            print("HTML соответствует исходникам")
        else:
            OUTPUT.write_text(result, encoding="utf-8")
            print(f"Собрана карта: {OUTPUT.relative_to(ROOT)}")
        return 0
    except (ValueError, KeyError, OSError) as exc:
        print(f"Не удалось собрать карту: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
