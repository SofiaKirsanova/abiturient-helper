#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import uuid
from pathlib import Path
from typing import Dict, List, Any

REPO_ROOT = Path(__file__).resolve().parents[1]

UNMATCHED_AGG = REPO_ROOT / "data/processed/aggregators_clean/unmatched_aggregators.json"
OUT_PATH = REPO_ROOT / "data/processed/universities.aggregators_candidates.json"

VUZ_KEYWORDS = ("университет", "академ", "институт", "консерват", "политех", "высш", "школа")

SUBUNIT_PREFIX_RE = re.compile(r"^(факультет|школа|юридический институт|институт)\b", flags=re.IGNORECASE)

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[«»\"'`]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_like_vuz(name: str) -> bool:
    n = norm(name)
    return any(k in n for k in VUZ_KEYWORDS)

def is_subunit(name: str) -> bool:
    n = norm(name)

    # 1) явный префикс подразделения
    if SUBUNIT_PREFIX_RE.match(name):
        return True

    # 2) подразделение внутри университета: "институт/школа/факультет ... университета ..."
    # Ключевое: именно "университета" (родительный), а не "университет"
    if "университета" in n:
        if re.search(r"\b(факультет|школа|институт)\b.+\bуниверситета\b", n):
            return True

    # 3) частный кейс: "университет (... институт)" — это НЕ подразделение
    # (МФТИ, МИФИ и подобные)
    if "университет" in n and re.search(r"университет\s*\(.*\bинститут\b.*\)", n):
        return False

    return False


def main() -> None:
    if not UNMATCHED_AGG.exists():
        raise SystemExit(f"Missing: {UNMATCHED_AGG}")

    items = json.load(UNMATCHED_AGG.open("r", encoding="utf-8"))
    if not isinstance(items, list):
        raise SystemExit("unmatched_aggregators.json must be a list")

    # group by normalized name
    by_name: Dict[str, Dict[str, Any]] = {}
    for it in items:
        src = (it.get("source") or "").strip()
        name = (it.get("name") or "").strip()
        if not name:
            continue

        # пропускаем совсем не-вузы (на всякий)
        if not looks_like_vuz(name) and not is_subunit(name):
            continue

        key = norm(name)
        rec = by_name.get(key)
        if rec is None:
            rec = {
                "id": str(uuid.uuid4()),
                "name": name,
                "name_norm": key,
                "sources": [],
                "review": True,
                "is_branch": False,
                "is_subunit": False,
            }
            by_name[key] = rec

        if src and src not in rec["sources"]:
            rec["sources"].append(src)

    # finalize flags
    out: List[Dict[str, Any]] = []
    for rec in by_name.values():
        rec["is_subunit"] = is_subunit(rec["name"])
        # branch правило: subunit => is_branch
        if rec["is_subunit"]:
            rec["is_branch"] = True
        out.append(rec)

    out.sort(key=lambda r: r["name_norm"])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, OUT_PATH.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)

    print(json.dumps({
        "input": str(UNMATCHED_AGG),
        "output": str(OUT_PATH),
        "candidates_total": len(out),
        "subunits_total": sum(1 for r in out if r["is_subunit"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
