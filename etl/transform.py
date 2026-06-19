"""In-memory processing with Polars, before loading to Postgres.

process() does the whole Transform step:
  - snake_case column names
  - trim whitespace, turn "" into null
  - best-effort numeric typing (skips identifier-like columns w/ leading zeros)
  - drop exact-duplicate rows
"""
from __future__ import annotations
import re

import polars as pl

_LEADING_ZERO = re.compile(r"^0[0-9]")


def _norm(name: str) -> str:
    n = re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()
    return n or "col"


def _maybe_numeric(s: pl.Series) -> pl.Series | None:
    """Return an Int64/Float64 cast if every non-null value parses; else None."""
    nn = s.drop_nulls()
    if nn.len() == 0:
        return None
    # keep identifier-like values (zip, codes) as text
    if nn.str.contains(_LEADING_ZERO.pattern).any():
        return None
    base_nulls = s.null_count()
    as_int = s.cast(pl.Int64, strict=False)
    if as_int.null_count() == base_nulls:
        return as_int
    as_float = s.cast(pl.Float64, strict=False)
    if as_float.null_count() == base_nulls:
        return as_float
    return None


def process(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    df = df.rename({c: _norm(c) for c in df.columns})

    str_cols = [c for c, t in zip(df.columns, df.dtypes) if t == pl.Utf8]
    if str_cols:
        df = df.with_columns(pl.col(str_cols).str.strip_chars())
        df = df.with_columns(
            pl.when(pl.col(str_cols).str.len_chars() == 0)
              .then(None).otherwise(pl.col(str_cols)).name.keep())

    casts = []
    for c in str_cols:
        out = _maybe_numeric(df[c])
        if out is not None:
            casts.append(out.alias(c))
    if casts:
        df = df.with_columns(casts)

    return df.unique(maintain_order=True)