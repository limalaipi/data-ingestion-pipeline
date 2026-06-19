# data-ingestion-pipeline

Simple, config-driven ETL for heterogeneous open-data sources (Socrata APIs,
source PostgreSQL, file/object downloads).

**Flow per dataset:  Extract → process in Polars (in-memory) → Load one clean
table into PostgreSQL.** No warehouse / medallion layering — data volume is
capped (default **5M rows per table**), so each source maps to a single typed
table.

The whole pipeline is driven by one registry file — `config/sources.yml`
(the code version of the Google Sheet). **Add a dataset = add a row there.**

## Layout

```
pyproject.toml          uv project (Python 3.11) + dependencies
.python-version         pins Python 3.11
main.py                 CLI runner (run without Airflow)
config/sources.yml      registry: generated from the Google Sheet
config/sources_extra.yml hand-curated additions (kept across syncs)
scripts/sync_registry.py regenerate sources.yml from the sheet
tests/                  pytest unit tests for process()
etl/
  config.py             load registry + merge defaults
  extract.py            dispatch to a connector by source type
  connectors/           socrata.py · postgres.py · file_download.py  (-> Polars)
  transform.py          process(): clean cols, trim, type, dedup (Polars)
  db.py                 engines + COPY a Polars frame into one table
  quality.py            row/column gates on the loaded table
  state.py              run log + optional watermarks (meta schema)
  pipeline.py           run one dataset: extract -> process -> load -> QA
dags/etl_by_source.py   one Airflow DAG per source group
ddl/00_schemas.sql      schema + meta DDL
```

## Output in Postgres

Schemas are organized by **topic domain**. Table names are `{origin}_{dataset}`
(e.g. `transport.nyc_collisions_person`, `environment.th_hii_rain_24_hist`), so
origin and the fine-grained `topic` tag are preserved without extra schemas.

| Schema          | Content (origins mixed; table prefix shows origin)        |
|-----------------|-----------------------------------------------------------|
| `environment`   | disaster, weather, air quality, IoT, nature, sanitation   |
| `transport`     | traffic, taxi, parking, mobility                          |
| `public_safety` | crime, enforcement, adjudication (OATH)                   |
| `urban`         | buildings, housing, land, property                        |
| `society`       | population, poverty, civic 311, government, business      |
| `meta`          | `etl_runs` (run log), `watermarks` (incremental)          |

Each table also carries audit cols `_loaded_at / _source / _batch_id`. Each run
**replaces** its target table with the latest (capped) data. DAGs are still
grouped by **origin** (`group`: th/nyc/chicago/files). The fine `topic` tag
(weather, civic, public_safety, property, …) stays in the registry for
sub-grouping and views.

## Processing (Polars)

`transform.process()` runs entirely in memory: snake_case column names · trim
whitespace · `""` → null · best-effort numeric typing (keeps identifier-like
values with leading zeros as text) · drop exact-duplicate rows.

## Test mode (~50 tables × ≤5M rows)

`config/sources.yml` → `defaults.row_limit: 5000000`, `sample: recent`, so each
dataset pulls only its **most recent ≤5M rows**. Override per dataset by adding
`row_limit:` to that entry (or `row_limit: null` for everything). No code change.

## Quick start (uv · Python 3.11)

```bash
uv python install 3.11
uv sync                       # create .venv + install deps + write uv.lock

cp .env.example .env          # fill in WAREHOUSE_URL + source URLs
psql "$WAREHOUSE_URL" -f ddl/00_schemas.sql

uv run python main.py --list
uv run python main.py --source nyc_collisions_person
uv run python main.py --group nyc
```

Dev tools + tests:

```bash
uv sync --group dev
uv run pytest -q
```

## Registry & sync

`config/sources.yml` is generated from the Google Sheet — regenerate after
editing the sheet:

```bash
uv run python scripts/sync_registry.py            # writes config/sources.yml
uv run python scripts/sync_registry.py --dry-run  # preview without writing
```

It classifies each row (postgres / socrata / file) and never touches
`config/sources_extra.yml`, which holds the 18 hand-picked NYC datasets
(≥20 cols, ≥1M rows). The loader merges both; `enabled: false` skips an entry.

## Airflow

Airflow is installed **separately** with its official constraints file:

```bash
uv pip install "apache-airflow==2.9.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
```

Put the repo on the Airflow `PYTHONPATH` and point `dags/` at it. You get one
DAG per group (`opendata_etl_nyc`, …), daily at 05:00, each dataset a task that
runs `extract → process → load → QA`.

## Incremental

Default is **full replace** of the most recent ≤5M rows each run. PostgreSQL
sources can opt into watermark incremental via
`incremental: {mode: watermark, column: <col>}`; watermarks live in
`meta.watermarks`.
