"""Load the source registry and merge defaults.

Two files make up the registry:
  config/sources.yml        generated from the Google Sheet (sync_registry.py)
  config/sources_extra.yml  hand-curated additions, preserved across syncs

Entries with `enabled: false` are skipped. If the same `name` appears in both
files, the entry in sources_extra.yml wins.
"""
from __future__ import annotations
import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

load_dotenv()

SOURCES_FILE = os.getenv("SOURCES_FILE", "config/sources.yml")
EXTRA_FILE = os.getenv("SOURCES_EXTRA_FILE", "config/sources_extra.yml")


def _resolve(path: str) -> Path:
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).resolve().parent.parent / p
    return p


def _read(path: str) -> tuple[dict, list[dict]]:
    p = _resolve(path)
    if not p.exists():
        return {}, []
    doc = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return doc.get("defaults", {}) or {}, doc.get("sources", []) or []


def load_registry(path: str | None = None) -> list[dict]:
    defaults, main = _read(path or SOURCES_FILE)
    _, extra = _read(EXTRA_FILE)

    merged: dict[str, dict] = {}
    for src in main + extra:                 # extra overrides same-named main
        if src.get("enabled", True) is False:
            merged.pop(src["name"], None)
            continue
        merged[src["name"]] = {**defaults, **src}

    result = list(merged.values())
    # local-test override: ROW_LIMIT env caps every dataset (e.g. main.py --limit 100)
    override = os.getenv("ROW_LIMIT")
    if override:
        for s in result:
            s["row_limit"] = int(override)
    return result


def get_source(name: str, path: str | None = None) -> dict:
    for s in load_registry(path):
        if s["name"] == name:
            return s
    raise KeyError(f"source '{name}' not found in registry")


def list_names(path: str | None = None) -> list[str]:
    return [s["name"] for s in load_registry(path)]
