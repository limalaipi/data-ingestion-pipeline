"""File / object connector: download CSV, ZIP-of-CSV, or JSON → Polars."""
from __future__ import annotations
import io
import zipfile

import polars as pl
import requests

# many open-data hosts reject the default python-requests UA with 403
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; opendata-etl/1.0)"}


def _dig(data, path: str | None):
    """Walk a dotted json_path (e.g. 'result.records'); None -> whole payload."""
    if not path:
        return data
    for key in path.split("."):
        data = data.get(key, []) if isinstance(data, dict) else []
    return data


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    url = cfg["url"]
    fmt = cfg.get("format", "csv")
    limit = cfg.get("row_limit")
    n = None if limit is None else int(limit)

    resp = requests.get(url, headers=HEADERS, timeout=300)
    resp.raise_for_status()

    if fmt == "csv":
        return pl.read_csv(io.BytesIO(resp.content), n_rows=n,
                           infer_schema_length=None)
    if fmt == "zip_csv":
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        member = cfg.get("zip_member") or next(
            m for m in zf.namelist() if m.lower().endswith((".csv", ".tsv")))
        sep = "\t" if member.lower().endswith(".tsv") else ","
        with zf.open(member) as fh:
            return pl.read_csv(fh.read(), separator=sep, n_rows=n,
                               infer_schema_length=None)
    if fmt == "json":
        rows = _dig(resp.json(), cfg.get("json_path"))
        df = pl.DataFrame(rows, infer_schema_length=None)
        return df.head(n) if n is not None else df
    raise ValueError(f"unsupported file format: {fmt}")
