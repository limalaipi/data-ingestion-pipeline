"""Source PostgreSQL connector (e.g. ISOC opendata, schemas op_*) → Polars."""
from __future__ import annotations
from typing import Iterator

import polars as pl

from ..db import source_engine


def _build_sql(cfg: dict, watermark: str | None) -> str:
    source = cfg["source"]                       # e.g. op_dopa.population_hist
    limit = cfg.get("row_limit")
    order_by = cfg.get("order_by")
    sample = cfg.get("sample", "recent")
    inc = cfg.get("incremental", {}) or {}

    where = ""
    if inc.get("mode") == "watermark" and watermark:
        where = f" WHERE {inc['column']} > '{watermark}'"
    order = ""
    if order_by:
        order = f" ORDER BY {order_by} {'DESC' if sample == 'recent' else 'ASC'}"
    lim = "" if limit is None else f" LIMIT {int(limit)}"
    return f"SELECT * FROM {source}{where}{order}{lim}"


def iter_batches(cfg: dict, watermark: str | None = None) -> Iterator[pl.DataFrame]:
    """Yield frames of ~page_size rows via a server-side batched read."""
    eng = source_engine(cfg["conn"])
    page = int(cfg.get("page_size", 20000))
    sql = _build_sql(cfg, watermark)
    # stream_results=True -> psycopg2 server-side cursor: rows arrive in chunks
    # instead of buffering the whole result client-side (true streaming + low RAM)
    with eng.connect().execution_options(stream_results=True) as cx:
        yield from pl.read_database(sql, connection=cx, iter_batches=True,
                                    batch_size=page, infer_schema_length=None)


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    """Non-streaming convenience: read the whole result into one frame."""
    eng = source_engine(cfg["conn"])
    with eng.connect() as cx:
        return pl.read_database(_build_sql(cfg, watermark), connection=cx,
                                infer_schema_length=None)
