"""Source PostgreSQL connector (e.g. ISOC opendata, schemas op_*) → Polars."""
from __future__ import annotations
import polars as pl

from ..db import source_engine


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    eng = source_engine(cfg["conn"])
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

    sql = f"SELECT * FROM {source}{where}{order}{lim}"
    with eng.connect() as cx:
        return pl.read_database(sql, connection=cx)
