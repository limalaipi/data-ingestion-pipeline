"""Dispatch a source config to the right connector (returns a Polars frame)."""
from __future__ import annotations
import polars as pl

from .connectors import socrata, postgres, file_download

_CONNECTORS = {
    "socrata": socrata.extract,
    "postgres": postgres.extract,
    "file": file_download.extract,
}


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    kind = cfg["type"]
    if kind not in _CONNECTORS:
        raise ValueError(f"unknown source type: {kind}")
    return _CONNECTORS[kind](cfg, watermark)
