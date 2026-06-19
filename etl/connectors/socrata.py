"""Socrata connector (NYC, Chicago, ...) → Polars.

Uses keyset pagination on the system :id column (`$order=:id` + `$where=:id > last`)
instead of deep `$offset`, so the server never rescans skipped rows — far faster
and avoids IncompleteRead / SSL EOF on large datasets. Transient network errors
are retried with exponential backoff.
"""
from __future__ import annotations
import logging
import os
import time

import polars as pl
import requests

log = logging.getLogger("etl.socrata")

# transient errors worth retrying (SSLError is a subclass of ConnectionError)
_TRANSIENT = (
    requests.exceptions.ConnectionError,
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


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    base = f"https://{cfg['domain']}/resource/{cfg['resource']}.json"
    headers = {}
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:                              # raises rate limits a lot
        headers["X-App-Token"] = token

    limit = cfg.get("row_limit")
    page = int(cfg.get("page_size", 20000))
    name = cfg.get("name", cfg["resource"])

    rows: list[dict] = []
    last_id: str | None = None
    with requests.Session() as sess:       # keep-alive across pages
        sess.headers.update(headers)
        while True:
            take = page if limit is None else min(page, limit - len(rows))
            if take <= 0:
                break
            params = {"$select": ":*, *", "$order": ":id", "$limit": take}
            if last_id is not None:
                params["$where"] = f":id > '{last_id}'"
            chunk = _get_json(sess, base, params)
            if not chunk:
                break
            last_id = chunk[-1].get(":id")
            # keep data columns only; ':' system fields are for paging
            rows.extend({k: v for k, v in rec.items() if not k.startswith(":")}
                        for rec in chunk)
            log.info("%s: fetched %d rows", name, len(rows))
            if len(chunk) < take or last_id is None:
                break

    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
