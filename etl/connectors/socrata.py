"""Socrata (NYC, Chicago, ...) connector via the SODA JSON API → Polars."""
from __future__ import annotations
import os
import polars as pl
import requests


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    base = f"https://{cfg['domain']}/resource/{cfg['resource']}.json"
    headers = {}
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    limit = cfg.get("row_limit")
    page = int(cfg.get("page_size", 50000))
    order_by = cfg.get("order_by")
    sample = cfg.get("sample", "recent")

    params: dict = {}
    if order_by:
        params["$order"] = f"{order_by} {'DESC' if sample == 'recent' else 'ASC'}"

    rows: list[dict] = []
    offset = 0
    while True:
        take = page if limit is None else min(page, limit - len(rows))
        if take <= 0:
            break
        resp = requests.get(base, params={**params, "$limit": take,
                            "$offset": offset}, headers=headers, timeout=120)
        resp.raise_for_status()
        chunk = resp.json()
        if not chunk:
            break
        rows.extend(chunk)
        offset += len(chunk)
        if len(chunk) < take:
            break

    # infer_schema_length=None => scan all rows so columns aren't missed
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
