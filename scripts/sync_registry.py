#!/usr/bin/env python3
"""Regenerate config/sources.yml from the Google Sheet registry.

The sheet is read via its CSV export URL (set SHEET_CSV_URL in .env, or pass
--url). Each row is classified into a connector config:

  * Host + db present            -> postgres  (conn from db, source = schema.table)
  * Source looks like xxxx-xxxx  -> socrata   (domain from `api` col or NYC default)
  * Source/api is an http URL    -> file
  * otherwise                    -> skipped (logged)

Hand-curated entries in config/sources_extra.yml are never touched.

Usage:
  uv run python scripts/sync_registry.py [--url CSV_URL] [--dry-run]
"""
from __future__ import annotations
import argparse
import csv
import io
import os
import re
import sys
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "config" / "sources.yml"
SOCRATA_ID = re.compile(r"^[a-z0-9]{4}-[a-z0-9]{4}$", re.I)

# db value in the sheet -> connection name used in .env (<NAME>_URL)
DB_TO_CONN = {"isoc_opendata": "isoc_opendata"}

DEFAULTS = {"row_limit": 5000000, "sample": "recent", "page_size": 50000}

HEADER = (
    "# Source Registry — GENERATED FROM THE GOOGLE SHEET\n"
    "# Regenerate with: uv run python scripts/sync_registry.py\n"
    "# Additions live in config/sources_extra.yml (not overwritten).\n"
)

# ISOC agency code -> subject topic tag
AGENCY_TOPIC = {
    "ddpm": "disaster", "dwr": "disaster", "hii": "weather", "wo_hii": "weather",
    "dopa": "population", "nesdc": "poverty", "opendata": "environment",
    "dga": "government", "gistda": "land",
}

# keyword -> topic, checked in order (transport before buildings so
# "parking_violations" -> transport, not buildings)
TOPIC_KEYWORDS = [
    (("311", "service_request"), "civic"),
    (("crash", "collision", "traffic", "taxi", "tlc", "parking", "tnp",
      "congestion", "transport", "vehicle", "trip", "fhv"), "transport"),
    (("crime", "arrest", "complaint", "nypd", "shooting"), "public_safety"),
    (("housing",), "housing"),
    (("dob", "permit", "building", "violation", "construction"), "buildings"),
    (("pluto", "valuation", "tax_lot", "landuse", "land"), "property"),
    (("rodent", "restaurant", "health"), "health"),
    (("business", "license"), "business"),
    (("oath", "contract", "hearing"), "government"),
    (("population", "house"), "population"),
    (("rain", "temperature", "water_level", "weather"), "weather"),
    (("disaster", "drought", "volunteer"), "disaster"),
    (("air_quality", "sensor", "environment"), "environment"),
    (("gbif", "specimen", "observation", "occurrence"), "nature"),
]


def guess_topic(name: str) -> str | None:
    n = name.lower()
    for keys, topic in TOPIC_KEYWORDS:
        if any(k in n for k in keys):
            return topic
    return None


# fine topic -> physical schema (domain). Unknown -> society (catch-all).
TOPIC_DOMAIN = {
    "disaster": "environment", "weather": "environment", "air_quality": "environment",
    "iot": "environment", "nature": "environment", "health": "environment",
    "environment": "environment",
    "transport": "transport",
    "public_safety": "public_safety", "justice": "public_safety",
    "buildings": "urban", "housing": "urban", "property": "urban", "land": "urban",
    "population": "society", "poverty": "society", "civic": "society",
    "government": "society", "business": "society",
}


def domain_of(topic: str | None) -> str:
    return TOPIC_DOMAIN.get(topic, "society")


def _slug(s: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", (s or "").lower()).strip("_") or "col"


def _get(row: dict, *keys: str) -> str:
    for k in keys:
        v = (row.get(k) or "").strip()
        if v:
            return v
    return ""


def classify(row: dict) -> dict | None:
    table = _get(row, "table")
    host = _get(row, "Host", "host")
    db = _get(row, "db")
    schema = _get(row, "schema")
    api = _get(row, "api")
    source = _get(row, "Source", "source")

    # 1) PostgreSQL source -> schema `th`, table prefixed by agency code
    if host and db and schema:
        conn = DB_TO_CONN.get(db, db)
        agency = schema[3:] if schema.startswith("op_") else schema
        topic = AGENCY_TOPIC.get(agency)
        name = f"th_{agency}_{_slug(table)}"
        return {
            "name": name,
            "group": "th",
            "topic": topic,
            "type": "postgres",
            "conn": conn,
            "source": f"{schema}.{table}",
            "exclude_prefix": "_isoc",   # drop ISOC system cols (_isoc_id uuid PK, ...)
            "target": {"schema": domain_of(topic), "table": name},
        }

    # 2) Socrata dataset (4x4 id in Source)
    if SOCRATA_ID.match(source):
        domain = api if "." in api else "data.cityofnewyork.us"
        origin = "chicago" if "chicago" in domain else "nyc"
        topic = guess_topic(table)
        name = f"{origin}_{_slug(table)}"
        return {
            "name": name,
            "group": origin,
            "topic": topic,
            "type": "socrata",
            "domain": domain,
            "resource": source,
            "target": {"schema": domain_of(topic), "table": name},
        }

    # 3) File / object (URL in Source or api)
    url = next((u for u in (source, api) if u.startswith("http")), "")
    if url:
        fmt = "zip_csv" if url.endswith(".zip") else "csv"
        topic = guess_topic(table) or "nature"
        return {
            "name": _slug(table),
            "group": "files",
            "topic": topic,
            "type": "file",
            "format": fmt,
            "url": url,
            "enabled": False,  # verify the URL is a direct file first
            "target": {"schema": domain_of(topic), "table": f"files_{_slug(table)}"},
        }

    return None


def fetch_rows(url: str) -> list[dict]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("SHEET_CSV_URL"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.url:
        sys.exit("set SHEET_CSV_URL in .env or pass --url")

    rows = fetch_rows(args.url)
    sources, skipped = [], []
    for r in rows:
        cfg = classify(r)
        (sources.append(cfg) if cfg else skipped.append(r))

    doc = {"defaults": DEFAULTS, "sources": sources}
    text = HEADER + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True,
                                   width=200)
    print(f"mapped {len(sources)} sources, skipped {len(skipped)}")
    if args.dry_run:
        print(text)
        return
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
