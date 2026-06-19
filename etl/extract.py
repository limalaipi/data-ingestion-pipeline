"""Dispatch a source config to the right connector (Polars frames)."""
from __future__ import annotations
from typing import Iterator

import polars as pl

from .connectors import socrata, postgres, file_download

_CONNECTORS = {
    "socrata": socrata,
    "postgres": postgres,
    "file": file_download,
}


def _connector(cfg: dict):
    kind = cfg["type"]
    if kind not in _CONNECTORS:
        raise ValueError(f"unknown source type: {kind}")
    return _CONNECTORS[kind]


def iter_batches(cfg: dict, watermark: str | None = None) -> Iterator[pl.DataFrame]:
    """Stream the source page by page (one frame per batch)."""
    return _connector(cfg).iter_batches(cfg, watermark)


def extract(cfg: dict, watermark: str | None = None) -> pl.DataFrame:
    """Whole source in one frame (non-streaming convenience)."""
    return _connector(cfg).extract(cfg, watermark)
