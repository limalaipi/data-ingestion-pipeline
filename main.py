#!/usr/bin/env python3
"""CLI entry point — run the ETL without Airflow.

  uv run python main.py --list
  uv run python main.py --source nyc_collisions_person
  uv run python main.py --group nyc
  uv run python main.py --all
"""
from __future__ import annotations
import argparse
import logging
import os

from etl import config, db
from etl.pipeline import run_dataset


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="data-ingestion-pipeline ETL runner")
    ap.add_argument("--source", help="run a single dataset by name")
    ap.add_argument("--group", help="run all datasets in a group (nyc/chicago/isoc/files)")
    ap.add_argument("--all", action="store_true", help="run every dataset")
    ap.add_argument("--list", action="store_true", help="list registered datasets")
    ap.add_argument("--limit", type=int,
                    help="override row_limit for this run (e.g. 100 for local testing)")
    ap.add_argument("--clear", action="store_true",
                    help="drop target table(s) for the selected sources, then exit")
    args = ap.parse_args()

    if args.limit is not None:
        os.environ["ROW_LIMIT"] = str(args.limit)

    reg = config.load_registry()
    if args.list:
        for s in reg:
            print(f"{s['name']:40} {s['type']:9} group={s.get('group')}")
        return

    if args.source:
        targets = [args.source]
    elif args.group:
        targets = [s["name"] for s in reg if s.get("group") == args.group]
    elif args.all:
        targets = [s["name"] for s in reg]
    else:
        ap.error("specify --source, --group, --all, or --list")

    if args.clear:
        eng = db.warehouse_engine()
        for name in targets:
            t = config.get_source(name)["target"]
            db.drop_table(eng, t["schema"], t["table"])
            print(f"cleared {t['schema']}.{t['table']}")
        return

    for name in targets:
        print(run_dataset(name))


if __name__ == "__main__":
    main()
