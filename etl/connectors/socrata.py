"""Socrata connector (NYC, Chicago, ...) → Polars, streamed page by page.

Keyset pagination on the system :id column (`$order=:id` + `$where=:id > last`)
instead of deep `$offset`, so the server never rescans skipped rows — fast and
avoids IncompleteRead / SSL EOF on large datasets. Transient errors are retried
with exponential backoff. `iter_batches` yields one frame per page (constant RAM).
"""
from __future__ import annotations
import json
import logging
import os
import time
from typing import Iterator

import polars as pl
import requests

log = logging.getLogger("etl.socrata")


def _stringify(v):
    """Coerce every JSON value to text at ingest so a column that is int in one
    row and float (or nested) in another doesn't break frame construction."""
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False, default=str)
    return str(v)

_TRANSIENT = (
    requests.exceptions.ConnectionError,   # incl. SSLError
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)
_RETRY_STATUS = {429, 500, 502, 503, 504}


class _Retry(Exception):
    pass


def _get_json(sess: requests.Session, url: str, params: dict, retries: int = 6):
    for attempt in range(retries):
        try:
            r = sess.get(url, params=params, timeout=120)
            if r.status_code in _RETRY_STATUS:
                raise _Retry(f"HTTP {r.status_code}")
            r.raise_for_status()          # permanent 4xx -> raise, no retry
            return r.json()
        except (_TRANSIENT, _Retry) as e:
            if attempt == retries - 1:
                raise
            wait = min(2 ** attempt, 60)
            log.warning("socrata request failed (%s); retry %d/%d in %ss",
                        e, attempt + 1, retries, wait)
            time.sleep(wait)


def iter_batches(cfg: dict, watermark: str | None = None) -> Iterator[pl.DataFrame]:
    """Yield one Polars frame per page (keyset by :id)."""
    base = f"https://{cfg['domain']}/resource/{cfg['resource']}.json"
    headers = {}
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    limit = cfg.get("row_limit")
    page = int(cfg.get("page_size", 20000))
    name = cfg.get("name", cfg["resource"])

    fetched = 0
    last_id: str | None = None
    with requests.Session() as sess:
        sess.headers.update(headers)
        while True:
            take = page if limit is None else min(page, limit - fetched)
            if take <= 0:
                break
            params = {"$select": ":*, *", "$order": ":id", "$limit": take}
            if last_id is not None:
                params["$where"] = f":id > '{last_id}'"
            chunk = _get_json(sess, base, params)
            if not chunk:
                break
            last_id = chunk[-1].get(":id")
            recs = [{k: _stringify(v) for k, v in r.items() if not k.startswith(":")}
                    for r in chunk]
            fetched += len(chunk)
            log.info("%s: fetched %d rows", name, fetched)
            yield pl.DataFrame(recs, infer_schema_length=None)
            if len(chunk) < take or last_id is None:
                break


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    """Non-streaming convenience: concatenate all pages into one frame."""
    frames = [b for b in iter_batches(cfg, watermark) if not b.is_empty()]
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
