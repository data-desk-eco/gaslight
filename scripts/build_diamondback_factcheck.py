#!/usr/bin/env python3
"""Build a fact-check package for a piece based on gaslight data.

Reads a YAML manifest of claims and data-export instructions, runs each
claim's SQL against the DuckDB, exports source tables to Excel + parquet,
writes a human-readable README, and zips the result for mailing to a
fact-checker.

Usage:
    uv run scripts/build_diamondback_factcheck.py \
        --manifest analysis/diamondback/factcheck/claims.yaml \
        --db       data/data.duckdb \
        --out      analysis/diamondback/factcheck_package.zip
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import textwrap
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import yaml

# openpyxl rejects Excel's illegal chars (most control chars). Strip them
# from any string column before writing xlsx.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def sanitise_for_xlsx(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.replace(_ILLEGAL_XLSX, "", regex=True)
    return df


def run_query(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return conn.execute(sql).fetch_df()


def export_table(
    conn: duckdb.DuckDBPyConnection, spec: dict, out_dir: Path
) -> tuple[int, Path, Path | None]:
    name = spec["name"]
    table = spec["table"]
    where = spec.get("where")
    cols = spec.get("columns", "*")
    order_by = spec.get("order_by")

    select = f"SELECT {cols} FROM {table}"
    if where:
        select += f" WHERE {where}"
    if order_by:
        select += f" ORDER BY {order_by}"

    # DuckDB writes parquet natively — no pyarrow dep needed
    parquet_path = out_dir / f"{name}.parquet"
    conn.execute(
        f"COPY ({select}) TO '{parquet_path}' (FORMAT PARQUET)"
    )
    rowcount = conn.execute(
        f"SELECT count(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]

    # Skip xlsx if the spec says so (huge tables) or it'd blow Excel's limit.
    xlsx_path: Path | None = None
    if spec.get("no_xlsx") or rowcount > 1_000_000:
        pass
    else:
        df = run_query(conn, select)
        xlsx_path = out_dir / f"{name}.xlsx"
        sanitise_for_xlsx(df).to_excel(xlsx_path, index=False, sheet_name=name[:31])

    return rowcount, parquet_path, xlsx_path


def render_bootstrap(exports: list[dict]) -> str:
    """Emit a bootstrap.sql that aliases original schema.table names to the
    bundled parquets, so the per-claim SQL files run as-shipped."""
    lines = [
        "-- Bootstrap: creates views that let the queries in queries/ run",
        "-- against the parquets in data/ without needing the full gaslight",
        "-- database. Run this once per DuckDB session before any query.",
        "--",
        "-- Usage:",
        "--   duckdb -c \".read bootstrap.sql\" -c \".read queries/04_permits_fang.sql\"",
        "",
    ]
    # Group by the schema implied by parquet_alias to emit CREATE SCHEMA
    schemas = set()
    for e in exports:
        if not (alias := e.get("parquet_alias")):
            continue
        if "." in alias:
            schemas.add(alias.split(".", 1)[0])
    for schema in sorted(schemas):
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    lines.append("")
    for e in exports:
        if not (alias := e.get("parquet_alias")):
            continue
        lines.append(
            f"CREATE OR REPLACE VIEW {alias} AS "
            f"SELECT * FROM read_parquet('data/{e['name']}.parquet');"
        )
    return "\n".join(lines) + "\n"


def format_result(df: pd.DataFrame) -> str:
    """Format query result as a compact markdown table (or pre-block if wide)."""
    if df.empty:
        return "*(no rows)*"
    # For single-cell scalars, just print the value
    if df.shape == (1, 1):
        val = df.iloc[0, 0]
        return f"**{val:,}**" if isinstance(val, (int, float)) else f"**{val}**"
    # Truncate big results
    truncated = df.head(50)
    md = truncated.to_markdown(index=False, floatfmt=",.2f")
    if len(df) > 50:
        md += f"\n\n*…{len(df) - 50:,} more rows in CSV*"
    return md


def render_readme(
    manifest: dict,
    claim_results: list[dict],
    exports: list[dict],
) -> str:
    lines: list[str] = []
    lines.append(f"# {manifest.get('title', 'Fact-check package')}")
    lines.append("")
    if intro := manifest.get("intro"):
        lines.append(intro.strip())
        lines.append("")
    lines.append("## Contents")
    lines.append("")
    lines.append("- `README.md` — this file")
    lines.append("- `data/` — near-source data files (Excel + Parquet)")
    lines.append("- `queries/` — one SQL file per claim")
    lines.append("- `results/` — the output of each query as a CSV")
    lines.append("")
    lines.append("## How to use this package")
    lines.append("")
    lines.append(textwrap.dedent("""\
        For each claim below you have three routes to verification:

        1. **Trust the computed value.** Each claim's SQL query was run
           against the live database at package-build time; the result is
           shown inline. If that matches the article's figure, you're done.
        2. **Reproduce from Excel.** Each claim has a plain-English recipe
           ("open this file, filter this column"). This is the fastest route
           for simple counts and filters.
        3. **Re-run the SQL yourself.** Install [DuckDB]
           (https://duckdb.org/docs/installation) — it's a single binary
           with no dependencies — then from the package root run:

           ```
           duckdb -c ".read bootstrap.sql" -c ".read queries/04_permits_fang.sql"
           ```

           `bootstrap.sql` sets up the schema aliases so the queries find
           the bundled parquets. Every claim in this package can be re-run
           this way — nothing requires the full upstream database.

        The article's figures are a snapshot taken at writing time. The
        database keeps being updated with new permit filings, plume
        detections, and production data, so live re-runs may return
        *larger* numbers than the article cites. The **shape** of the
        comparison (FANG vs Apache vs Pioneer) should hold.
    """))
    lines.append("")
    lines.append("## Pipeline")
    lines.append("")
    lines.append(textwrap.dedent("""\
        The diagram below traces where every figure in the article comes from.
        Grey boxes are external sources we don't control; blue boxes are raw
        files landed verbatim from those sources; green boxes are normalised
        tables built by SQL in the `gaslight` repo (`queries/load.sql`,
        `queries/rrc.sql`); yellow boxes are the files bundled in this
        package. Each transformation is a SQL view or COPY statement — no
        hidden Python munging, no ML, no imputation.
    """).rstrip())
    lines.append("")
    lines.append("```mermaid")
    lines.append(textwrap.dedent("""\
        flowchart TD
            classDef src fill:#e5e5e5,stroke:#888,color:#222;
            classDef raw fill:#cfe2f3,stroke:#3d85c6,color:#0b3d66;
            classDef norm fill:#d9ead3,stroke:#6aa84f,color:#274e13;
            classDef bundle fill:#fff2cc,stroke:#bf9000,color:#5b4500;

            RRC_SWR32[RRC SWR 32 permits<br/>web database]:::src
            RRC_MFT[RRC MFT<br/>EBCDIC well + operator files]:::src
            RRC_PDQ[RRC PDQ<br/>monthly gas disposition]:::src
            EOG_VNF[EOG VIIRS Nightfire<br/>satellite flare detections]:::src
            CM[Carbon Mapper API<br/>methane plumes]:::src
            IMEO[UNEP IMEO<br/>methane plumes]:::src

            PERMIT_HTML[raw/permit_details.csv<br/>+ permit_properties.csv<br/>+ permit_locations.csv]:::raw
            WELLS_CSV[raw/wells.csv<br/>raw/operators.csv<br/>raw/gatherers.csv]:::raw
            PROD_CSV[raw/lease_production.csv]:::raw
            VNF_CSV[raw/vnf.csv]:::raw
            PLUMES_CSV[raw/plumes.csv<br/>cm + imeo merged]:::raw

            RRC_PERMITS[(rrc.permits<br/>one row per permit,<br/>dates parsed, status + reasons)]:::norm
            RRC_PROD[(rrc.production<br/>monthly flared/produced MCF<br/>per lease, operator attributed)]:::norm
            RAW_WELLS[(raw.wells<br/>joined to operators<br/>via operator_no)]:::norm
            RAW_PLUMES[(raw.plumes<br/>unified schema<br/>cm + imeo)]:::norm
            RAW_VNF[(raw.vnf<br/>flare site profiles)]:::norm

            PKG_PERMITS[/data/permits.parquet/]:::bundle
            PKG_PROD[/data/production_flaring.parquet/]:::bundle
            PKG_WELLS[/data/raw_wells.parquet/]:::bundle
            PKG_PLUMES_F[/data/plumes_fang.parquet/]:::bundle
            PKG_PLUMES_A[/data/raw_plumes_all.parquet/]:::bundle
            PKG_VNF[/data/raw_vnf.parquet/]:::bundle
            PKG_FL[/data/raw_permit_locations.parquet/]:::bundle

            RRC_SWR32 -- scrape_permits.py<br/>scrape_permit_details.py --> PERMIT_HTML
            RRC_MFT -- download_rrc.py<br/>parse_rrc.py --> WELLS_CSV
            RRC_PDQ -- pdq export --> PROD_CSV
            EOG_VNF -- fetch_vnf.py --> VNF_CSV
            CM -- fetch_plumes.py --> PLUMES_CSV
            IMEO -- manual geojson --> PLUMES_CSV

            PERMIT_HTML -- load.sql + rrc.sql<br/>merge filings + detail pages,<br/>parse dates --> RRC_PERMITS
            PROD_CSV -- load.sql + rrc.sql<br/>sum disposition code 04 --> RRC_PROD
            WELLS_CSV -- load.sql --> RAW_WELLS
            PLUMES_CSV -- load.sql --> RAW_PLUMES
            VNF_CSV -- load.sql --> RAW_VNF

            RRC_PERMITS --> PKG_PERMITS
            RRC_PROD --> PKG_PROD
            RAW_WELLS --> PKG_WELLS
            RAW_PLUMES -- 375m proximity filter<br/>to FANG/Endeavor wells --> PKG_PLUMES_F
            RAW_PLUMES --> PKG_PLUMES_A
            RAW_VNF --> PKG_VNF
            PERMIT_HTML --> PKG_FL
    """).rstrip())
    lines.append("```")
    lines.append("")
    lines.append(textwrap.dedent("""\
        **Rendering:** GitHub renders Mermaid diagrams inline in `.md` files,
        so opening this README on github.com shows the chart directly. For a
        standalone view, paste the block above into <https://mermaid.live>.

        **Key design points worth knowing for fact-checking:**

        - **Spatial matching uses a fixed 375m radius** (the VIIRS satellite
          pixel half-width). A ±0.0034° lat/lon bounding box is applied first
          as a cheap pre-filter, then exact distance is computed. Any claim
          involving "within 375m" or "nearby" uses this.
        - **Operator attribution is string-based.** We match operators by
          exact `operator_name` or `operator_number` from RRC records. For
          Diamondback we confirmed only `DIAMONDBACK E&P LLC` and
          `ENDEAVOR ENERGY RESOURCES L.P.` appear as filers in permits or
          production — no Viper, no Rattler, no other subsidiaries file
          directly. Claims combining FANG + Endeavor sum these two strings.
        - **No imputation.** If a permit has no effective date, it stays
          null — we never guess. If a lease's monthly production is missing,
          it's excluded from the denominator for that month.
        - **Permian filter** (applied at export): lat 30°–33.5°N,
          lon -104.5° to -100°W, plus a Texas-only rule (above 32°N must be
          east of -103.064° to exclude New Mexico).
    """).rstrip())
    lines.append("")
    lines.append("## Data files")
    lines.append("")
    for e in exports:
        desc = (e.get("description") or "").strip()
        rows = e["rowcount"]
        lines.append(
            f"### `data/{e['name']}.xlsx` / `data/{e['name']}.parquet`"
            f" — {rows:,} rows"
        )
        lines.append("")
        if desc:
            lines.append(desc)
            lines.append("")
        lines.append(f"Source: `{e['table']}`")
        if e.get("where"):
            lines.append(f"Filter applied: `{e['where']}`")
        lines.append("")

    lines.append("## Claims")
    lines.append("")
    by_section: dict[str, list[dict]] = {}
    for cr in claim_results:
        by_section.setdefault(cr["section"], []).append(cr)

    for section, items in by_section.items():
        lines.append(f"### {section}")
        lines.append("")
        for cr in items:
            lines.append(f"#### {cr['index']}. {cr['figure']}")
            lines.append("")
            lines.append("> " + cr["quote"].strip().replace("\n", "\n> "))
            lines.append("")
            if cr.get("requires_full_db"):
                lines.append(
                    "> ⚠️ **Requires full database.** This query references "
                    "a table too large to bundle (VIIRS detections or "
                    "lease-level production). You can verify via the "
                    "computed value and the Excel recipe, but not by "
                    "re-running the SQL against the bundle."
                )
                lines.append("")
            lines.append("**Computed value:** " + cr["result_md"])
            lines.append("")
            if cr.get("howto"):
                lines.append("**How to recreate in Excel:** " + cr["howto"].strip())
                lines.append("")
            lines.append(
                f"**SQL:** [`queries/{cr['sql_file']}`](queries/{cr['sql_file']}) "
                f"— full results in [`results/{cr['csv_file']}`](results/{cr['csv_file']})"
            )
            lines.append("")
            lines.append("```sql")
            lines.append(cr["sql"].strip())
            lines.append("```")
            lines.append("")

    return "\n".join(lines)


_MERMAID_BLOCK = re.compile(r"```mermaid\n(.*?)\n```", re.DOTALL)


def render_pdf(readme_md: str, build_dir: Path) -> None:
    """Render the README to PDF via pandoc + xelatex.

    Pre-renders the Mermaid flowchart to PNG using mermaid-cli (npx), so the
    PDF gets an embedded image instead of a code block the TeX engine can't
    interpret. Falls back to skipping PDF if the toolchain is missing.
    """
    if not shutil.which("pandoc"):
        print("  pandoc not found — skipping PDF render")
        return

    pdf_md = readme_md
    match = _MERMAID_BLOCK.search(readme_md)
    if match:
        mmd_path = build_dir / "etl_diagram.mmd"
        png_path = build_dir / "etl_diagram.png"
        mmd_path.write_text(match.group(1) + "\n")
        print("  rendering Mermaid diagram → PNG…", flush=True)
        r = subprocess.run(
            [
                "npx", "-y", "@mermaid-js/mermaid-cli",
                "-i", str(mmd_path), "-o", str(png_path),
                "-b", "white", "-s", "2",
            ],
            capture_output=True, text=True,
        )
        if r.returncode != 0 or not png_path.exists():
            print(f"    mermaid-cli failed; leaving code block in PDF:\n{r.stderr}")
        else:
            pdf_md = _MERMAID_BLOCK.sub(
                f"![ETL pipeline](etl_diagram.png){{ width=100% }}", pdf_md, count=1
            )

    pdf_md_path = build_dir / "README_for_pdf.md"
    pdf_md_path.write_text(pdf_md)
    pdf_path = build_dir / "README.pdf"

    print("  rendering PDF via pandoc…", flush=True)
    r = subprocess.run(
        [
            "pandoc", pdf_md_path.name, "-o", pdf_path.name,
            "--pdf-engine=xelatex",
            "--toc", "--toc-depth=2",
            "-V", "geometry:margin=2cm",
            "-V", "colorlinks=true",
            "-V", "linkcolor=blue",
            "-V", "urlcolor=blue",
            "-V", "mainfont=Helvetica",
            "-V", "monofont=Menlo",
            "--no-highlight",
        ],
        capture_output=True, text=True, cwd=build_dir,
    )
    if r.returncode != 0:
        print(f"    pandoc failed:\n{r.stderr[-2000:]}")
    else:
        print(f"    → {pdf_path.name} ({pdf_path.stat().st_size / 1e6:.1f} MB)")
    pdf_md_path.unlink(missing_ok=True)


def build(manifest_path: Path, db_path: Path, out_zip: Path) -> None:
    manifest = yaml.safe_load(manifest_path.read_text())
    build_dir = out_zip.parent / (out_zip.stem + "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)
    (build_dir / "data").mkdir(parents=True)
    (build_dir / "queries").mkdir()
    (build_dir / "results").mkdir()

    conn = duckdb.connect(str(db_path), read_only=True)

    # --- Data exports ------------------------------------------------------
    exports: list[dict] = []
    for spec in manifest.get("data_exports", []):
        print(f"  exporting {spec['name']}…", flush=True)
        rows, parquet, xlsx = export_table(conn, spec, build_dir / "data")
        exports.append({**spec, "rowcount": rows})
        fmt = f"{xlsx.name}, {parquet.name}" if xlsx else f"{parquet.name} only"
        print(f"    → {rows:,} rows → {fmt}")

    # --- Claims ------------------------------------------------------------
    claim_results: list[dict] = []
    for i, claim in enumerate(manifest.get("claims", []), start=1):
        slug = claim["id"]
        sql_file = f"{i:02d}_{slug}.sql"
        csv_file = f"{i:02d}_{slug}.csv"
        print(f"  running claim {i}: {slug}…", flush=True)

        sql = claim["sql"]
        (build_dir / "queries" / sql_file).write_text(
            f"-- {claim.get('figure', slug)}\n"
            f"-- Quote: {claim['quote'].strip()}\n\n"
            f"{sql.strip()}\n"
        )

        df = run_query(conn, sql)
        df.to_csv(build_dir / "results" / csv_file, index=False)

        claim_results.append({
            "index": i,
            "id": slug,
            "section": claim.get("section", "Claims"),
            "quote": claim["quote"],
            "figure": claim.get("figure", slug),
            "sql": sql,
            "howto": claim.get("howto", ""),
            "sql_file": sql_file,
            "csv_file": csv_file,
            "result_md": format_result(df),
        })

    # --- Bootstrap + README ------------------------------------------------
    (build_dir / "bootstrap.sql").write_text(render_bootstrap(exports))
    readme = render_readme(manifest, claim_results, exports)
    (build_dir / "README.md").write_text(readme)
    render_pdf(readme, build_dir)

    # --- Zip ---------------------------------------------------------------
    if out_zip.exists():
        out_zip.unlink()
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in build_dir.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(build_dir.parent))
    print(f"\nWrote {out_zip} ({out_zip.stat().st_size / 1e6:.1f} MB)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--db", type=Path, default=Path("data/data.duckdb"))
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("analysis/diamondback/factcheck_package.zip"),
    )
    args = ap.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    build(args.manifest, args.db, args.out)


if __name__ == "__main__":
    main()
