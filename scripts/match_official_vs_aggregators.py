#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any

REPO_ROOT = Path(__file__).resolve().parents[1]

OFFICIAL_PATH = REPO_ROOT / "data/processed/universities.unique.json"

AGG_DIR = REPO_ROOT / "data/processed/aggregators_clean"
AGG_SOURCES = {
    "postupi": AGG_DIR / "postupi.names.clean.json",
    "tabiturient": AGG_DIR / "tabiturient.names.clean.json",
    "ucheba": AGG_DIR / "ucheba.names.clean.json",
    # subunits keep отдельно — не в union, чтобы не мешать (но можно анализировать)
    "ucheba_subunits": AGG_DIR / "ucheba.subunits.json",
}

OUT_DIR = AGG_DIR
OUT_UNION = OUT_DIR / "aggregators_union.json"
OUT_OFFICIAL_ENRICHED = OUT_DIR / "official_enriched.json"
OUT_UNMATCHED_AGG = OUT_DIR / "unmatched_aggregators.json"
OUT_REPORT = OUT_DIR / "match_report.json"


# ----------------- IO -----------------

def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ----------------- normalization -----------------

NBSP = "\u00a0"

STOP_PHRASES_RE = [
    # юридические/организационные хвосты
    r"\bфедеральное\b",
    r"\bгосударственное\b",
    r"\bбюджетное\b",
    r"\bавтономное\b",
    r"\bказенное\b",
    r"\bчастное\b",
    r"\bнекоммерческ\w*\b",
    r"\bобразовательн\w*\b",
    r"\bучреждени\w*\b",
    r"\bорганизаци\w*\b",
    r"\bвысшего\s+образования\b",
    r"\bпрофессионального\s+образования\b",
    r"\bво\b",  # ФГБОУ ВО и т.п. — лучше убирать на нормализации
    r"\bфгбоу\b",
    r"\bфгаоу\b",
    r"\bаоу\b",
    r"\bоу\b",
]

PUNCT_RE = re.compile(r"""[.,;:!?'"“”«»\(\)\[\]{}]""")
DASH_RE = re.compile(r"[–—−]")
MULTISPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^0-9a-zа-яё\s-]+" , flags=re.IGNORECASE)

def norm_text(s: str) -> str:
    s = (s or "").replace(NBSP, " ").strip()
    s = DASH_RE.sub("-", s)
    s = PUNCT_RE.sub(" ", s)
    s = NON_ALNUM_RE.sub(" ", s)
    s = s.replace("ё", "е").lower()
    s = MULTISPACE_RE.sub(" ", s).strip()
    return s

def strip_stop_phrases(s: str) -> str:
    t = norm_text(s)
    for rx in STOP_PHRASES_RE:
        t = re.sub(rx, " ", t, flags=re.IGNORECASE)
    t = MULTISPACE_RE.sub(" ", t).strip()
    return t

def official_key(name: str) -> str:
    """
    Ключ для official: убираем юридические фразы, оставляем смысловое ядро.
    """
    t = strip_stop_phrases(name)
    # убрать "министерства ... российской федерации" как шум (часто не нужен для совпадения)
    t = re.sub(r"\bминистерств\w*\b.*?\bроссийск\w*\s+федераци\w*\b", " ", t)
    t = MULTISPACE_RE.sub(" ", t).strip()
    return t

def agg_key(name: str) -> str:
    """
    Ключ для агрегаторов: проще, но тем же стилем.
    """
    t = strip_stop_phrases(name)
    return t


BRANCH_RE = re.compile(r"\b(филиал|представительств\w*|отделени\w*)\b", flags=re.IGNORECASE)
SUBUNIT_RE = re.compile(r"^\s*(факультет|школа|кафедра|институт)\b", flags=re.IGNORECASE)

def is_branch_name(s: str) -> bool:
    return bool(BRANCH_RE.search(s or ""))

def looks_like_subunit(s: str) -> bool:
    sl = (s or "").lower()
    return bool(SUBUNIT_RE.match(sl)) or ("университета" in sl and any(x in sl for x in ["факультет", "школа", "институт"]))


# ----------------- fuzzy -----------------

def _get_fuzzy():
    try:
        from rapidfuzz import fuzz, process
        return fuzz, process
    except Exception:
        return None, None

FUZZ, PROCESS = _get_fuzzy()

def best_fuzzy(query: str, choices: List[str]) -> Tuple[str, float]:
    """
    Возвращает (best_choice, score). Если rapidfuzz нет — fallback на 0.
    """
    if not query or not choices:
        return "", 0.0

    if FUZZ is None or PROCESS is None:
        return "", 0.0

    # token_set_ratio хорошо переносит перестановки слов
    res = PROCESS.extractOne(
        query,
        choices,
        scorer=FUZZ.token_set_ratio,
    )
    if not res:
        return "", 0.0
    best, score, _idx = res
    return best, float(score)


# ----------------- main logic -----------------

def build_aggregators_union() -> Tuple[List[Dict], Dict[str, List[str]]]:
    """
    union_list: [{"name":..., "source":...}, ...]
    by_source_clean: source -> list[str]
    """
    by_source: Dict[str, List[str]] = {}
    union = []
    for src, path in AGG_SOURCES.items():
        if not path.exists():
            continue
        data = load_json(path)
        if not isinstance(data, list):
            continue
        names = [x for x in data if isinstance(x, str)]
        by_source[src] = names
        if src == "ucheba_subunits":
            continue  # не кладем в union
        for n in names:
            union.append({"name": n, "source": src})
    return union, by_source


def main():
    official = load_json(OFFICIAL_PATH)
    if not isinstance(official, list):
        raise ValueError("universities.unique.json must be a list")

    union, by_source = build_aggregators_union()
    save_json(OUT_UNION, union)

    agg_names = [x["name"] for x in union]
    agg_keys = [agg_key(n) for n in agg_names]

    # map: normalized_key -> list of indices in agg_names
    key_to_idxs: Dict[str, List[int]] = {}
    for i, k in enumerate(agg_keys):
        key_to_idxs.setdefault(k, []).append(i)

    matched_agg_idx = set()

    enriched = []
    exact_matches = 0
    fuzzy_matches = 0
    none_matches = 0
    review_count = 0
    branch_count = 0

    # для fuzzy поиска
    # (ключи агрегаторов как "choices")
    choices = list(dict.fromkeys(agg_keys))  # unique keep order
    key_to_first_idx = {}
    for i, k in enumerate(agg_keys):
        if k not in key_to_first_idx:
            key_to_first_idx[k] = i

    for rec in official:
        if not isinstance(rec, dict):
            continue
        org = rec.get("organization_name", "") or ""
        ok = official_key(org)

        out = dict(rec)
        out["match"] = {
            "official_key": ok,
            "found_in_aggregators": False,
            "match_type": "none",
            "match_score": 0.0,
            "matched_name": "",
            "aggregator_sources": [],
            "review": False,
            "is_branch": False,
        }

        out["match"]["is_branch"] = is_branch_name(org)

        # 1) exact by key
        if ok in key_to_idxs:
            idxs = key_to_idxs[ok]
            matched_agg_idx.update(idxs)
            out["match"]["found_in_aggregators"] = True
            out["match"]["match_type"] = "exact"
            out["match"]["match_score"] = 100.0

            # взять "самое короткое" имя из агрегаторов как display
            names = [agg_names[i] for i in idxs]
            names_sorted = sorted(names, key=len)
            out["match"]["matched_name"] = names_sorted[0]

            # sources
            srcs = sorted({union[i]["source"] for i in idxs})
            out["match"]["aggregator_sources"] = srcs

            exact_matches += 1
        else:
            # 2) fuzzy
            best_key, score = best_fuzzy(ok, choices)
            if score >= 90 and best_key:
                # подтянем все agg индексы с этим best_key
                idxs = key_to_idxs.get(best_key, [])
                if idxs:
                    matched_agg_idx.update(idxs)

                out["match"]["found_in_aggregators"] = True
                out["match"]["match_type"] = "fuzzy"
                out["match"]["match_score"] = score
                out["match"]["matched_name"] = agg_names[key_to_first_idx[best_key]]
                out["match"]["aggregator_sources"] = sorted({union[i]["source"] for i in idxs}) if idxs else []
                fuzzy_matches += 1

                # review если fuzzy не очень уверенный
                if score < 96:
                    out["match"]["review"] = True
            else:
                none_matches += 1

        # review если агрегаторное имя выглядит как суб-юнит/филиал
        mn = out["match"]["matched_name"]
        if mn and looks_like_subunit(mn):
            out["match"]["review"] = True
        if mn and is_branch_name(mn):
            out["match"]["is_branch"] = True

        if out["match"]["review"]:
            review_count += 1
        if out["match"]["is_branch"]:
            branch_count += 1

        enriched.append(out)

    # unmatched aggregators (в union) — чего нет среди official матчей
    unmatched = []
    for i, row in enumerate(union):
        if i in matched_agg_idx:
            continue
        nm = row["name"]
        unmatched.append({
            "name": nm,
            "source": row["source"],
            "key": agg_key(nm),
        })

    report = {
        "official_total": len(enriched),
        "aggregators_union_total": len(union),
        "matched_exact": exact_matches,
        "matched_fuzzy": fuzzy_matches,
        "matched_total": exact_matches + fuzzy_matches,
        "unmatched_official": none_matches,
        "unmatched_aggregators": len(unmatched),
        "review_count": review_count,
        "branch_count": branch_count,
        "sources": {k: len(v) for k, v in by_source.items()},
        "paths": {
            "official": str(OFFICIAL_PATH),
            "aggregators_clean_dir": str(AGG_DIR),
        }
    }

    save_json(OUT_OFFICIAL_ENRICHED, enriched)
    save_json(OUT_UNMATCHED_AGG, unmatched)
    save_json(OUT_REPORT, report)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
