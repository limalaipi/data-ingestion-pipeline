"""Run logging + incremental watermarks, stored in the `meta` schema."""
from __future__ import annotations
import uuid
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import ensure_schema


def init_meta(engine: Engine) -> None:
    ensure_schema(engine, "meta")
    with engine.begin() as cx:
        cx.execute(text(
            "CREATE TABLE IF NOT EXISTS meta.watermarks ("
            "source_name text PRIMARY KEY, watermark text, "
            "updated_at timestamptz)"))
        cx.execute(text(
            "CREATE TABLE IF NOT EXISTS meta.etl_runs ("
            "batch_id text PRIMARY KEY, source_name text, status text, "
            "rows bigint, started_at timestamptz, finished_at timestamptz, "
            "message text)"))


def new_batch_id() -> str:
    return uuid.uuid4().hex[:12]


def get_watermark(engine: Engine, source_name: str) -> str | None:
    with engine.begin() as cx:
        r = cx.execute(text(
            "SELECT watermark FROM meta.watermarks WHERE source_name=:n"),
            {"n": source_name}).fetchone()
    return r[0] if r else None


def set_watermark(engine: Engine, source_name: str, value: str | None) -> None:
    if value is None:
        return
    with engine.begin() as cx:
        cx.execute(text(
            "INSERT INTO meta.watermarks (source_name, watermark, updated_at) "
            "VALUES (:n,:w,:t) ON CONFLICT (source_name) DO UPDATE "
            "SET watermark=:w, updated_at=:t"),
            {"n": source_name, "w": value,
             "t": datetime.now(timezone.utc)})


def start_run(engine: Engine, batch_id: str, source_name: str) -> None:
    with engine.begin() as cx:
        cx.execute(text(
            "INSERT INTO meta.etl_runs (batch_id, source_name, status, "
            "started_at) VALUES (:b,:n,'running',:t)"),
            {"b": batch_id, "n": source_name,
             "t": datetime.now(timezone.utc)})


def finish_run(engine: Engine, batch_id: str, status: str,
               rows: int = 0, message: str = "") -> None:
    with engine.begin() as cx:
        cx.execute(text(
            "UPDATE meta.etl_runs SET status=:s, rows=:r, finished_at=:t, "
            "message=:m WHERE batch_id=:b"),
            {"s": status, "r": rows, "t": datetime.now(timezone.utc),
             "m": message[:2000], "b": batch_id})
