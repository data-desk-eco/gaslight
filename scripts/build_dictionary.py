#!/usr/bin/env python3
"""Generate the gaslight data dictionary from docs/data-dictionary/_meta.yaml.

Introspects dist/gaslight.duckdb for the real columns, types, and row counts,
then writes:
  - docs/data-dictionary/README.md      (index + overview)
  - docs/data-dictionary/<table>.md      (one per table)
  - _dictionary, _sources                (metadata tables inside the DB)

The YAML is the only thing authored by hand; the markdown is always generated.
A column in the DB but missing from the YAML (or vice-versa) prints a warning.

Usage:
    uv run scripts/build_dictionary.py
    uv run scripts/build_dictionary.py --db dist/gaslight.duckdb
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import yaml

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs" / "data-dictionary"


def fmt(n: int) -> str:
    return f"{n:,}"


def db_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name NOT LIKE '\\_%' ESCAPE '\\' "
        "ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]


def db_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return [(c[0], c[1]) for c in con.execute(f'DESCRIBE "{table}"').fetchall()]


def db_rowcount(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]


def build_metadata_tables(con, meta, tables, columns, rowcounts):
    """(Re)create the in-DB _dictionary and _sources tables."""
    con.execute("DROP TABLE IF EXISTS _dictionary")
    con.execute(
        "CREATE TABLE _dictionary ("
        "table_name VARCHAR, column_name VARCHAR, data_type VARCHAR, description VARCHAR)"
    )
    dict_rows = []
    for t in tables:
        col_desc = meta["tables"].get(t, {}).get("columns", {})
        for name, dtype in columns[t]:
            dict_rows.append((t, name, dtype, col_desc.get(name, "")))
    con.executemany("INSERT INTO _dictionary VALUES (?, ?, ?, ?)", dict_rows)

    con.execute("DROP TABLE IF EXISTS _sources")
    con.execute(
        "CREATE TABLE _sources ("
        "id VARCHAR, name VARCHAR, provider VARCHAR, url VARCHAR, "
        "license VARCHAR, retrieval VARCHAR)"
    )
    con.executemany(
        "INSERT INTO _sources VALUES (?, ?, ?, ?, ?, ?)",
        [
            (s["id"], s["name"], s.get("provider", ""), s.get("url", ""),
             s.get("license", ""), s.get("retrieval", ""))
            for s in meta["sources"]
        ],
    )


def source_names(meta) -> dict[str, str]:
    return {s["id"]: s["name"] for s in meta["sources"]}


def write_table_doc(t, meta, cols, rows, names):
    spec = meta["tables"][t]
    srcs = ", ".join(names.get(s, s) for s in spec.get("source", []))
    out = [f"# `{t}`", ""]
    out += [spec["purpose"].strip(), ""]
    out += [f"- **Grain:** {spec['grain']}"]
    out += [f"- **Rows:** {fmt(rows)}"]
    out += [f"- **Source:** {srcs}"]
    if spec.get("scope"):
        out += [f"- **Scope:** {spec['scope'].strip()}"]
    out += ["", "## Schema", "", "| column | type | description |", "| --- | --- | --- |"]
    col_desc = spec.get("columns", {})
    for name, dtype in cols:
        desc = col_desc.get(name, "").strip().replace("\n", " ")
        out.append(f"| `{name}` | {dtype} | {desc} |")
    if spec.get("caveats"):
        out += ["", "## Caveats", "", spec["caveats"].strip()]
    if spec.get("example"):
        out += ["", "## Example", "", "```sql", spec["example"].strip(), "```"]
    out += ["", "---", "", "[← back to index](README.md)", ""]
    (DOCS / f"{t}.md").write_text("\n".join(out) + "\n")


def write_readme(meta, tables, columns, rowcounts, names):
    d = meta["dataset"]
    out = [f"# {d['title']}", "", f"*{d['subtitle']}*", ""]
    out += [d["intro"].strip(), ""]
    out += ["> [!IMPORTANT]"]
    out += ["> " + d["key_caveat"].strip().replace("\n", "\n> "), ""]
    out += ["## Scope", "", d["scope"].strip(), ""]
    out += ["## Method notes", "", d["method_notes"].strip(), ""]
    out += ["## How to use", "", d["how_to_use"].strip(), ""]

    out += ["## Tables", "", "| table | grain | rows | what it is |", "| --- | --- | --- | --- |"]
    for t in tables:
        spec = meta["tables"][t]
        summary = spec.get("summary", spec["purpose"].strip().split("\n")[0]).strip()
        out.append(
            f"| [`{t}`]({t}.md) | {spec['grain']} | {fmt(rowcounts[t])} | {summary} |"
        )
    out += [""]

    out += ["## Sources & attribution", ""]
    for s in meta["sources"]:
        out += [
            f"- **{s['name']}** — {s.get('provider', '')}. "
            f"{s.get('license', '')} "
            f"<{s.get('url', '')}>"
        ]
    out += [
        "",
        "All RRC data are Texas public records. Satellite and methane layers are "
        "free/open with attribution as noted — verify current provider terms before "
        "republishing derived products.",
        "",
        "*This dictionary is generated from `_meta.yaml`; the same content populates "
        "the `_dictionary` and `_sources` tables inside the database.*",
        "",
    ]
    (DOCS / "README.md").write_text("\n".join(out) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(REPO / "dist" / "gaslight.duckdb"))
    ap.add_argument("--meta", default=str(DOCS / "_meta.yaml"))
    args = ap.parse_args()

    meta = yaml.safe_load(Path(args.meta).read_text())
    con = duckdb.connect(args.db)

    tables = db_tables(con)
    columns = {t: db_columns(con, t) for t in tables}
    rowcounts = {t: db_rowcount(con, t) for t in tables}
    names = source_names(meta)

    # Validate meta vs DB
    documented = set(meta["tables"])
    for t in tables:
        if t not in documented:
            print(f"  ⚠️  table '{t}' in DB but not in _meta.yaml")
            continue
        meta_cols = set(meta["tables"][t].get("columns", {}))
        db_cols = {c for c, _ in columns[t]}
        for c in db_cols - meta_cols:
            print(f"  ⚠️  {t}.{c} in DB but undocumented in _meta.yaml")
        for c in meta_cols - db_cols:
            print(f"  ⚠️  {t}.{c} documented but not in DB")
    for t in documented - set(tables):
        print(f"  ⚠️  table '{t}' documented but not in DB")

    # Order doc tables by the YAML order, then any extras alphabetically
    ordered = [t for t in meta["tables"] if t in tables]
    ordered += [t for t in tables if t not in ordered]

    DOCS.mkdir(parents=True, exist_ok=True)
    build_metadata_tables(con, meta, ordered, columns, rowcounts)
    write_readme(meta, ordered, columns, rowcounts, names)
    for t in ordered:
        if t in meta["tables"]:
            write_table_doc(t, meta, columns[t], rowcounts[t], names)

    con.close()
    print(f"  data dictionary → {DOCS.relative_to(REPO)}/ ({len(ordered)} tables)")


if __name__ == "__main__":
    main()
