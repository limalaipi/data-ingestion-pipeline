"""End-to-end run for one dataset:  Extract -> Polars process -> Load -> QA.

Used by both the Airflow DAG and the local CLI (run_local.py).
Each run REPLACES the target table with the latest (capped) data. Optional
watermark incremental is honored for postgres sources whose `incremental.column`
is present in the data.
"""
from __future__ import annotations
import logging

from . import config, db, state, transform, quality, extract as ex

log = logging.getLogger("etl.pipeline")


def _columns_to_drop(cfg: dict, columns: list[str]) -> list[str]:
    """Resolve columns to exclude, from `exclude_columns` (exact names) and
    `exclude_prefix` (drop anything starting with the prefix, e.g. `_isoc`)."""
    drop = set(cfg.get("exclude_columns") or [])
    prefix = cfg.get("exclude_prefix")
    if prefix:
        drop |= {c for c in columns if c.startswith(prefix)}
    return [c for c in columns if c in drop]


def run_dataset(name: str) -> dict:
    cfg = config.get_source(name)
    eng = db.warehouse_engine()
    state.init_meta(eng)

    batch_id = state.new_batch_id()
    state.start_run(eng, batch_id, name)
    log.info("[%s] start batch=%s", name, batch_id)
    try:
        inc = cfg.get("incremental", {}) or {}
        wm = state.get_watermark(eng, name) if inc.get("mode") == "watermark" else None

        raw = ex.extract(cfg, wm)
        log.info("[%s] extracted %d rows", name, raw.height)
        if raw.is_empty():
            state.finish_run(eng, batch_id, "success", 0, "no rows")
            return {"source": name, "rows": 0, "status": "success"}

        drop_cols = _columns_to_drop(cfg, raw.columns)
        if drop_cols:
            raw = raw.drop(drop_cols)
            log.info("[%s] dropped columns: %s", name, drop_cols)

        df = transform.process(raw)
        t = cfg["target"]
        n = db.load_table(eng, df, t["schema"], t["table"], name, batch_id)
        report = quality.check_table(eng, t["schema"], t["table"])

        # advance watermark from the (normalized) incremental column if present
        col = (inc.get("column") or "").lstrip(":").lower()
        if inc.get("mode") == "watermark" and col in df.columns:
            state.set_watermark(eng, name, str(df[col].max()))

        state.finish_run(eng, batch_id, "success", n, f"loaded={n} qa={report}")
        log.info("[%s] done rows=%d", name, n)
        return {"source": name, "rows": n, "status": "success",
                "batch_id": batch_id}
    except Exception as e:  # noqa: BLE001
        state.finish_run(eng, batch_id, "failed", 0, str(e))
        log.exception("[%s] failed", name)
        raise