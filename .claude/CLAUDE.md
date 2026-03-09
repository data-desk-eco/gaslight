# Data Desk Research Notebooks

Data Desk publishes investigative research as interactive notebooks using Observable Notebook Kit 2.0. Notebooks are standalone HTML pages with embedded JavaScript that compile to static sites.

## File structure

```
repo/
├── docs/
│   ├── index.html           # Notebook source (EDIT THIS)
│   ├── assets/              # Images
│   .observable/dist/        # Built output (gitignored)
├── data/                    # DuckDB, CSV, JSON files
│   └── data.duckdb          # Main database, queried from the notebook
├── scripts/                 # Scripts for building the database, e.g. by fetching data and etl'ing it
├── template.html            # HTML wrapper (auto-updates from .github repo)
├── Makefile
├── CLAUDE.md                # Optional project-specific instructions
└── .claude/CLAUDE.md        # Shared instructions (auto-updates from .github repo)
```

**Commit:** `docs/index.html`, `data/*`, `docs/assets/*`, `Makefile`, `CLAUDE.md` (if project-specific)
**Don't commit:** `docs/.observable/dist/`, `node_modules/`, `template.html`, `.claude/`

## Technical philosophy

Data Desk values minimal, elegant solutions:
- **DuckDB SQL** for all data transformation, analysis, and aggregation
- **Shell scripts with Unix tools** (curl, jq, sed, awk, make) for data fetching and pipeline orchestration
- Keep code concise; avoid over-engineering

## Style guide

- Use **sentence case** for all titles, headings, and chart titles (e.g., "Outages by country" not "Outages By Country")
- *Always* use SQL cells interacting with the main DuckDB database for SQ

## Critical gotchas

1. **Data paths:** Use `../data/` from notebook, not `data/`
2. **SQL database path:** `database="../data/data.duckdb"` in SQL cells
3. **Display everything:** Use `display()` explicitly, don't rely on return values
4. **Cell IDs:** Must be unique across notebook
5. **Await FileAttachment:** All FileAttachment calls return promises
6. **Edit source:** Edit `docs/index.html`, not `docs/.observable/dist/`
7. **Auto-updating files:** `template.html` and `.claude/` download from `.github` repo on deploy
8. **Case-sensitive paths:** GitHub Pages is case-sensitive
9. **SQL cells at build time:** Database must exist when running `make build`
10. **`-- SQL comments` get misinterpreted by the notebook builder, please avoid 

## Resources

- Observable Notebook Kit: https://observablehq.com/notebook-kit/
- Observable Plot: https://observablehq.com/plot/
- Observable Inputs: https://observablehq.com/notebook-kit/inputs
- DuckDB SQL: https://duckdb.org/docs/sql/introduction
- All Data Desk notebooks: https://research.datadesk.eco/
