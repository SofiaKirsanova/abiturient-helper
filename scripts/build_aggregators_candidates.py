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

# subunit — начинаются с явных структурных единиц
SUBUNIT_PREFIX_RE = re.compile(
    r"^(\S+\s+){0,3}(факультет|школа|юридический\s+институт)\b",
    flags=re.IGNORECASE
)

# “институт …” как subunit будем ловить отдельным правилом ниже
INSTITUTE_WORD_RE = re.compile(r"\bинститут\b", flags=re.IGNORECASE)

# признаки принадлежности к "родительскому" вузу
BELONGING_MARKERS = [
    "университета", "академии",
    "ранхигс", "мэи", "мгу", "рудн", "вшэ", "мифи", "мфти",
]

# самостоятельные вузы, которые начинаются с "Институт ..."
INDEPENDENT_INSTITUTES_RAW = [
    "институт международных экономических связей",
    "международный институт экономики и права",
]

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[«»\"'`()]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
INDEPENDENT_INSTITUTES = {norm(x) for x in INDEPENDENT_INSTITUTES_RAW}

def detect_is_subunit(name: str) -> bool:
    s = norm(name)

    # независимые институты — не subunit
    if s in INDEPENDENT_INSTITUTES:
        return False

    # 1) явный префикс subunit (факультет/школа/юридический институт)
    if SUBUNIT_PREFIX_RE.match(s):
        return True

    # 2) “институт …” считаем subunit ТОЛЬКО если есть маркер принадлежности
    # и при этом это не выглядит как самостоятельный “московский ... институт”
    if s.startswith("московский ") and "институт" in s and "университета" not in s and "академии" not in s:
        return False

    if INSTITUTE_WORD_RE.search(s) and any(m in s for m in BELONGING_MARKERS):
        return True

    return False



def looks_like_vuz(name: str) -> bool:
    n = norm(name)
    return any(k in n for k in VUZ_KEYWORDS)


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

        # фильтр "на всякий": оставляем либо вузы, либо то что выглядит как subunit
        if not looks_like_vuz(name) and not detect_is_subunit(name):
            continue

        key = norm(name)
        is_sub_now = detect_is_subunit(name)

        rec = by_name.get(key)
        if rec is None:
            rec = {
                "id": str(uuid.uuid4()),
                "name": name,
                "name_norm": key,
                "sources": [],
                # правила для агрегаторов:
                "review": True,
                "is_subunit": is_sub_now,
                "is_branch": is_sub_now,
            }
            by_name[key] = rec
        else:
            # КЛЮЧЕВОЕ: если хоть один вариант говорит "subunit", фиксируем это
            if is_sub_now:
                rec["is_subunit"] = True
                rec["is_branch"] = True

            # необязательно, но полезно: выбрать "лучшее" имя, если текущее выглядит хуже
            # (например, короче/чище)
            if len(name) < len(rec["name"]):
                rec["name"] = name

        if src and src not in rec["sources"]:
            rec["sources"].append(src)

    out: List[Dict[str, Any]] = list(by_name.values())
    out.sort(key=lambda r: r["name_norm"])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, OUT_PATH.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
        # --- sanity checks (anti-regressions) ---
    # 1) "факультет/школа/юридический институт" обязаны быть subunit, иначе регресс
    rx_strict = re.compile(r"^(\S+\s+){0,3}(факультет|школа|юридический\s+институт)\b", re.I)
    strict_bad = [r for r in out if rx_strict.search(r["name"]) and not r.get("is_subunit")]
    if strict_bad:
        raise SystemExit(
            "Sanity check failed: strict subunits not marked as is_subunit:\n"
            + "\n".join(f"- {r['name']}" for r in strict_bad[:20])
        )

    # 2) subunit не должен выглядеть как отдельный 'московский ... институт' без маркеров принадлежности
    susp = []
    for r in out:
        if not r.get("is_subunit"):
            continue
        n = norm(r["name"])
        if n.startswith("московский ") and "институт" in n and ("университета" not in n and "академии" not in n):
            susp.append(r)
    if susp:
        raise SystemExit(
            "Sanity check failed: suspicious subunits:\n"
            + "\n".join(f"- {r['name']}" for r in susp[:20])
        )

    print(json.dumps({
        "input": str(UNMATCHED_AGG),
        "output": str(OUT_PATH),
        "candidates_total": len(out),
        "subunits_total": sum(1 for r in out if r.get("is_subunit")),
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
