"""Warehouse helpers: engines + load a processed Polars frame via COPY."""
from __future__ import annotations
import io
import os
from datetime import datetime, timezone

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Polars dtype -> PostgreSQL column type
_PG_TYPE = {
    pl.Int8: "smallint", pl.Int16: "smallint", pl.Int32: "integer",
    pl.Int64: "bigint", pl.UInt8: "integer", pl.UInt16: "integer",
    pl.UInt32: "bigint", pl.UInt64: "bigint",
    pl.Float32: "double precision", pl.Float64: "double precision",
    pl.Boolean: "boolean", pl.Date: "date",
    pl.Datetime: "timestamp", pl.Utf8: "text", pl.String: "text",
}


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


def _pg_type(dtype) -> str:
    return _PG_TYPE.get(dtype, "text")


def load_table(engine: Engine, df: pl.DataFrame, schema: str, table: str,
               source_name: str, batch_id: str) -> int:
    """Replace schema.table with the processed frame (+ audit columns)."""
    ensure_schema(engine, schema)
    loaded_at = datetime.now(timezone.utc).isoformat()
    df = df.with_columns([
        pl.lit(loaded_at).alias("_loaded_at"),
        pl.lit(source_name).alias("_source"),
        pl.lit(batch_id).alias("_batch_id"),
    ])

    coldefs = [f'"{c}" {_pg_type(t)}' for c, t in zip(df.columns, df.dtypes)]
    coldefs[-3] = '"_loaded_at" timestamptz'      # override the audit cols
    fq = f'"{schema}"."{table}"'
    with engine.begin() as cx:
        cx.execute(text(f"DROP TABLE IF EXISTS {fq}"))
        cx.execute(text(f"CREATE TABLE {fq} ({', '.join(coldefs)})"))

    buf = io.StringIO(df.write_csv(include_header=False))
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        collist = ", ".join(f'"{c}"' for c in df.columns)
        cur.copy_expert(f"COPY {fq} ({collist}) FROM STDIN WITH (FORMAT csv)", buf)
        raw.commit()
        cur.close()
    finally:
        raw.close()
    return df.height