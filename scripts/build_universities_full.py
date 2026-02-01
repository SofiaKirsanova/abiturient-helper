#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from typing import Dict, Any, List

REPO_ROOT = Path(__file__).resolve().parents[1]

OFFICIAL = REPO_ROOT / "data/processed/universities.unique.json"
CANDIDATES = REPO_ROOT / "data/processed/universities.aggregators_candidates.json"
OUT_FULL = REPO_ROOT / "data/processed/universities.full.json"

def norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[«»\"'`]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main() -> None:
    off = json.load(OFFICIAL.open("r", encoding="utf-8"))
    cand = json.load(CANDIDATES.open("r", encoding="utf-8"))

    # index official by normalized organization_name
    off_idx: Dict[str, Dict[str, Any]] = {}
    for r in off:
        k = norm(r.get("organization_name", ""))
        if not k:
            continue
        rr = dict(r)
        rr["source"] = "official"
        rr["review"] = False
        rr["is_branch"] = False
        rr["aggregator_sources"] = []
        off_idx[k] = rr

    # merge
    merged: List[Dict[str, Any]] = list(off_idx.values())
    added = 0
    for c in cand:
        k = c.get("name_norm") or norm(c.get("name", ""))
        if not k:
            continue
        if k in off_idx:
            # если есть official — только добавим источники агрегаторов (для удобства аудита)
            off_idx[k]["aggregator_sources"] = sorted(set(off_idx[k].get("aggregator_sources", []) + c.get("sources", [])))
            continue

        merged.append({
            "id": c.get("id"),
            "organization_name": c.get("name"),
            "source": "aggregator",
            "review": bool(c.get("review", True)),
            "is_branch": bool(c.get("is_branch", False)),
            "aggregator_sources": sorted(set(c.get("sources", []))),
        })
        added += 1

    merged.sort(key=lambda r: norm(r.get("organization_name", "")))

    OUT_FULL.parent.mkdir(parents=True, exist_ok=True)
    json.dump(merged, OUT_FULL.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)

    print(json.dumps({
        "official_total": len(off),
        "candidates_total": len(cand),
        "full_total": len(merged),
        "added_from_candidates": added,
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
