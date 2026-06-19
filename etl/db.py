"""Warehouse helpers: engines + load a processed Polars frame via COPY."""
from __future__ import annotations
import io
import os
from datetime import datetime, timezone

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# fail fast instead of hanging ~2 min when a source DB is unreachable (e.g. VPN down)
CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))


def warehouse_engine() -> Engine:
    return create_engine(os.environ["WAREHOUSE_URL"], future=True,
                         connect_args={"connect_timeout": CONNECT_TIMEOUT})


def source_engine(conn_name: str) -> Engine:
    return create_engine(os.environ[f"{conn_name.upper()}_URL"], future=True,
                         connect_args={"connect_timeout": CONNECT_TIMEOUT})


def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as cx:
        cx.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def drop_table(engine: Engine, schema: str, table: str) -> None:
    """Clear a target table (used by `main.py --clear`)."""
    with engine.begin() as cx:
        cx.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}"'))


def begin_table(engine: Engine, schema: str, table: str, columns: list[str]) -> None:
    """(Re)create schema.table as all-text columns + audit cols (replace mode)."""
    ensure_schema(engine, schema)
    fq = f'"{schema}"."{table}"'
    coldefs = ", ".join(f'"{c}" text' for c in columns)
    with engine.begin() as cx:
        cx.execute(text(f"DROP TABLE IF EXISTS {fq}"))
        cx.execute(text(
            f'CREATE TABLE {fq} ({coldefs}, "_loaded_at" timestamptz, '
            f'"_source" text, "_batch_id" text)'))


def add_columns(engine: Engine, schema: str, table: str, columns: list[str]) -> None:
    """Add columns a later page introduced (sparse sources); all as text."""
    fq = f'"{schema}"."{table}"'
    with engine.begin() as cx:
        existing = {r[0] for r in cx.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=:s AND table_name=:t"), {"s": schema, "t": table})}
        for c in columns:
            if c not in existing:
                cx.execute(text(f'ALTER TABLE {fq} ADD COLUMN "{c}" text'))


def copy_append(engine: Engine, df: pl.DataFrame, schema: str, table: str,
                source_name: str, batch_id: str) -> int:
    """Append one processed (all-text) batch via COPY — constant memory per page."""
    loaded_at = datetime.now(timezone.utc).isoformat()
    df = df.with_columns([
        pl.lit(loaded_at).alias("_loaded_at"),
        pl.lit(source_name).alias("_source"),
        pl.lit(batch_id).alias("_batch_id"),
    ])
    buf = io.StringIO(df.write_csv(include_header=False))
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        collist = ", ".join(f'"{c}"' for c in df.columns)
        cur.copy_expert(
            f'COPY "{schema}"."{table}" ({collist}) FROM STDIN WITH (FORMAT csv)', buf)
        raw.commit()
        cur.close()
    finally:
        raw.close()
    return df.height