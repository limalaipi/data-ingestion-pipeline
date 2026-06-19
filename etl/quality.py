"""Lightweight data-quality gates run on the loaded table."""
from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Engine


class QualityError(Exception):
    pass


def check_table(engine: Engine, schema: str, table: str,
                min_rows: int = 1) -> dict:
    fq = f'"{schema}"."{table}"'
    with engine.begin() as cx:
        rows = cx.execute(text(f"SELECT count(*) FROM {fq}")).scalar()
        ncols = cx.execute(text(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema=:s AND table_name=:t"),
            {"s": schema, "t": table}).scalar()

    report = {"rows": int(rows), "columns": int(ncols),
              "passed": True, "failures": []}
    if rows < min_rows:
        report["passed"] = False
        report["failures"].append(f"rows {rows} < min {min_rows}")
    if ncols < 1:
        report["passed"] = False
        report["failures"].append("no columns")
    if not report["passed"]:
        raise QualityError(f"{schema}.{table}: {report['failures']}")
    return report
