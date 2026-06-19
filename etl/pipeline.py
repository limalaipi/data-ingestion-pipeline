"""End-to-end run for one dataset, STREAMED page by page:
   extract batch -> process (all-text) -> COPY append -> repeat.

Constant memory regardless of table size. Each run REPLACES the target table
(first batch drops+creates; later batches append). Columns land as text — cast
in SQL at query time. Optional watermark incremental is honored for postgres.
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
        wm_col = (inc.get("column") or "").lstrip(":").lower()
        schema, table = cfg["target"]["schema"], cfg["target"]["table"]

        total = 0
        created = False
        next_wm = None
        for raw in ex.iter_batches(cfg, wm):       # one page at a time
            drop_cols = _columns_to_drop(cfg, raw.columns)
            if drop_cols:
                raw = raw.drop(drop_cols)
            df = transform.process(raw, to_text=True)
            if df.is_empty():
                continue
            if not created:
                db.begin_table(eng, schema, table, df.columns)   # drop + create
                created = True
            else:
                db.add_columns(eng, schema, table, df.columns)   # sparse new cols
            total += db.copy_append(eng, df, schema, table, name, batch_id)
            log.info("[%s] loaded %d rows so far", name, total)
            if inc.get("mode") == "watermark" and wm_col in df.columns:
                m = df[wm_col].max()
                next_wm = m if next_wm is None else max(next_wm, m)

        if not created:
            state.finish_run(eng, batch_id, "success", 0, "no rows")
            return {"source": name, "rows": 0, "status": "success"}

        report = quality.check_table(eng, schema, table)
        if inc.get("mode") == "watermark" and next_wm is not None:
            state.set_watermark(eng, name, str(next_wm))

        state.finish_run(eng, batch_id, "success", total, f"loaded={total} qa={report}")
        log.info("[%s] done rows=%d", name, total)
        return {"source": name, "rows": total, "status": "success",
                "batch_id": batch_id}
    except Exception as e:  # noqa: BLE001
        state.finish_run(eng, batch_id, "failed", 0, str(e))
        log.exception("[%s] failed", name)
        raise