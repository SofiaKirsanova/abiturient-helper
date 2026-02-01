#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set

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
OUT_FUZZY_REJECTED = OUT_DIR / "fuzzy_rejected.json"


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
    r"\bво\b",  # ФГБОУ ВО и т.п.
    r"\bфгбоу\b",
    r"\bфгаоу\b",
    r"\bаоу\b",
    r"\bоу\b",
]

STOP_PHRASES_RE += [
    r"\bвоенное\b",
    r"\bвоенная\b",
    r"\bвооруженных\b",
    r"\bсил\b",
    r"\bроссии\b",
    r"\bгород[ао]?\s+москвы\b",
    r"\bнауки\b",
    r"\bфедеральный\s+исследовательский\s+центр\b",
    r"\bордена\b",
    r"\bтрудового\b",
    r"\bкрасного\b",
    r"\bзнамени\b",
]


PUNCT_RE = re.compile(r"""[.,;:!?'"“”«»\(\)\[\]{}]""")
DASH_RE = re.compile(r"[–—−]")
MULTISPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^0-9a-zа-яё\s-]+", flags=re.IGNORECASE)

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
    # убрать "министерства ... российской федерации" как шум
    t = re.sub(r"\bминистерств\w*\b.*?\bроссийск\w*\s+федераци\w*\b", " ", t)
    t = MULTISPACE_RE.sub(" ", t).strip()
    return t

def agg_key(name: str) -> str:
    """
    Ключ для агрегаторов: проще, но тем же стилем.
    """
    return strip_stop_phrases(name)


# ----------------- branch / subunit heuristics -----------------

BRANCH_RE = re.compile(r"\b(филиал|представительств\w*|отделени\w*)\b", flags=re.IGNORECASE)
SUBUNIT_RE = re.compile(r"^\s*(факультет|школа|кафедра|институт)\b", flags=re.IGNORECASE)

def is_branch_name(s: str) -> bool:
    return bool(BRANCH_RE.search(s or ""))

def looks_like_subunit(s: str) -> bool:
    sl = (s or "").lower()
    return bool(SUBUNIT_RE.match(sl)) or (
        "университета" in sl and any(x in sl for x in ["факультет", "школа", "институт"])
    )


# ----------------- fuzzy guard (anti false positives) -----------------

GENERIC_TOKENS: Set[str] = {
    "университет", "институт", "академия", "школа", "училище", "колледж",
    "государственный", "федеральный", "национальный", "исследовательский",
    "высший", "высшего", "образования", "учреждение", "автономный",
    "бюджетный", "казенный", "частный", "некоммерческий",
    "московский", "российский", "рф", "имени",
    "министерства", "российской", "федерации",
}

ANCHOR_TOKENS: Set[str] = {
    "ранхигс", "мэи", "миэт", "мгюа", "мгу", "вшэ", "мфти", "мифи",
    "рудн", "гцолифк",
}

def _tokenize_key(s: str) -> List[str]:
    t = norm_text(s)
    return [x for x in t.split() if x]

def _significant_tokens(key: str) -> Set[str]:
    toks = _tokenize_key(key)
    return {x for x in toks if x not in GENERIC_TOKENS and len(x) >= 3}

def _passes_fuzzy_guard(query_key: str, best_key: str, score: float) -> bool:
    """
    Защита от ложных fuzzy-матчей.
    - если score >= 97: достаточно >= 1 общих значимых токена
    - иначе: нужно >= 2 общих значимых токена
    Плюс якоря: если в agg есть якорь, он должен быть и в query.
    """
    q_sig = _significant_tokens(query_key)
    b_sig = _significant_tokens(best_key)

    inter = len(q_sig & b_sig)
    min_inter = 1 if score >= 97 else 2
    if inter < min_inter:
        return False

    b_anchors = b_sig & ANCHOR_TOKENS
    q_anchors = q_sig & ANCHOR_TOKENS
    if b_anchors and not (b_anchors <= q_anchors):
        return False

    return True


# ----------------- official aliases -----------------

LEGAL_PREFIX_RE = re.compile(
    r"^(федеральн\w+\s+государственн\w+\s+"
    r"(автономн\w+|бюджетн\w+|казенн\w+)\s+"
    r"образовательн\w+\s+учрежден\w+\s+"
    r"(высшего\s+образования\s+)?)",
    flags=re.IGNORECASE
)

# вырезаем хвост "имени ..." (очень часто шум)
IMENI_RE = re.compile(r"\bим(ени)?\b.*$", flags=re.IGNORECASE)

# вытащим контент из «...» и "..."
QUOTES_RE = re.compile(r"[«\"]([^»\"]{2,80})[»\"]")
PARENS_RE = re.compile(r"\(([^)]{2,80})\)")

# запрещаем отдавать алиасы-одиночки типа "институт"
FORBIDDEN_ALIAS_KEYS: Set[str] = {
    "университет",
    "институт",
    "академия",
    "школа",
    "училище",
    "колледж",
}

def _compact(s: str) -> str:
    s = (s or "").replace(NBSP, " ").strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _keep_alias(raw_alias: str) -> bool:
    """
    Фильтр алиасов:
    - не короче 8 символов
    - key не должен быть одиночным общим словом ("институт" и т.д.)
    - key не должен быть супер-общим "<город> институт/университет..."
    """
    a = _compact(raw_alias)
    if not a or len(a) < 8:
        return False

    k = official_key(a)
    if not k:
        return False

    # 1) одиночные общие
    if k in FORBIDDEN_ALIAS_KEYS:
        return False

    toks = k.split()
    # 2) 1-2 слова и заканчивается на общий тип
    if len(toks) <= 2 and toks and toks[-1] in FORBIDDEN_ALIAS_KEYS:
        return False

    # 3) жестко выкидываем некоторые опасные "общие"
    GENERIC_BAD = {
        "московский университет",
        "российский университет",
        "московский институт",
        "российский институт",
        "московская академия",
        "российская академия",
    }
    if k in GENERIC_BAD:
        return False

    return True

def build_official_aliases(org_name: str) -> List[str]:
    """
    Возвращает список "сырьевых" вариантов названия (не key).
    - не теряем содержимое кавычек/скобок (делаем отдельные варианты)
    - делаем вариант без "имени ..."
    - НЕ добавляем алиасы вроде "институт"
    """
    base = (org_name or "").replace(NBSP, " ").strip()
    if not base:
        return []

    cand: List[str] = []

    def add(x: str) -> None:
        x = _compact(x)
        if _keep_alias(x):
            cand.append(x)

    # A) как есть
    add(base)

    # B) убрать юридический префикс
    b1 = LEGAL_PREFIX_RE.sub("", base).strip()
    add(b1)

    # C) убрать "имени ..."
    b2 = IMENI_RE.sub("", b1).strip()
    add(b2)

    # D) кавычки/скобки
    quoted = [m.group(1).strip() for m in QUOTES_RE.finditer(base)]
    parens = [m.group(1).strip() for m in PARENS_RE.finditer(base)]

    # вариант без символов кавычек/скобок
    no_qp = re.sub(r"[«»\"()]", " ", base)
    add(no_qp)

    # core для комбинирования с quoted/parens
    core = _compact(LEGAL_PREFIX_RE.sub("", base))
    core = re.sub(r"\([^)]*\)", " ", core)
    core = re.sub(r"[«»\"]", " ", core)
    core = _compact(core)

    for q in quoted:
        q2 = _compact(q)
        if not q2:
            continue
        # core + q
        if core:
            add(f"{core} {q2}")
        # отдельно q — только если это НЕ одиночное "институт/университет/..."
        add(q2)

    for p in parens:
        p2 = _compact(p)
        if not p2:
            continue
        if core:
            add(f"{core} {p2}")
        # отдельно p2 часто аббревиатура: "маи", "мгюа" — пусть будет (но фильтр отрежет слишком короткие)
        add(p2)

    # E) убрать "российской федерации/рф"
    for x in list(cand):
        y = re.sub(r"\b(российск\w*\s+федераци\w*|рф)\b", " ", x, flags=re.IGNORECASE)
        add(y)

    # уникализируем, сохраняя порядок
    seen = set()
    out: List[str] = []
    for x in cand:
        xl = x.lower()
        if xl in seen:
            continue
        seen.add(xl)
        out.append(x)

    out.sort(key=lambda x: (len(x), x.lower()))
    return out


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

    res = PROCESS.extractOne(query, choices, scorer=FUZZ.token_set_ratio)
    if not res:
        return "", 0.0
    best, score, _idx = res
    return best, float(score)


# ----------------- main logic -----------------

def build_aggregators_union() -> Tuple[List[Dict[str, str]], Dict[str, List[str]]]:
    """
    union_list: [{"name":..., "source":...}, ...]
    by_source_clean: source -> list[str]
    """
    by_source: Dict[str, List[str]] = {}
    union: List[Dict[str, str]] = []

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


def main() -> None:
    official = load_json(OFFICIAL_PATH)
    if not isinstance(official, list):
        raise ValueError("universities.unique.json must be a list")

    union, by_source = build_aggregators_union()
    save_json(OUT_UNION, union)

    agg_names = [x["name"] for x in union]
    agg_keys = [agg_key(n) for n in agg_names]

    # key -> indices in agg_names
    key_to_idxs: Dict[str, List[int]] = {}
    for i, k in enumerate(agg_keys):
        key_to_idxs.setdefault(k, []).append(i)

    matched_agg_idx = set()

    enriched: List[Dict[str, Any]] = []
    fuzzy_rejected: List[Dict[str, Any]] = []

    exact_matches = 0
    fuzzy_matches = 0
    none_matches = 0
    review_count = 0
    branch_count = 0

    # для fuzzy поиска: уникальные ключи агрегаторов
    choices = list(dict.fromkeys(agg_keys))  # unique keep order

    # best_key -> first index for display
    key_to_first_idx: Dict[str, int] = {}
    for i, k in enumerate(agg_keys):
        if k not in key_to_first_idx:
            key_to_first_idx[k] = i

    for rec in official:
        if not isinstance(rec, dict):
            continue

        org = rec.get("organization_name", "") or ""
        ok_main = official_key(org)

        aliases_raw = build_official_aliases(org)
        alias_pairs = [(a_raw, official_key(a_raw)) for a_raw in aliases_raw]

        out = dict(rec)
        out["match"] = {
            "official_key": ok_main,
            "official_aliases": aliases_raw,
            "official_alias_keys": [ak for (_a, ak) in alias_pairs if ak and ak != ok_main],
            "found_in_aggregators": False,
            "match_type": "none",
            "match_score": 0.0,
            "matched_name": "",
            "aggregator_sources": [],
            "review": False,
            "is_branch": is_branch_name(org),
            "matched_via": "",                         # "official_name" | "official_alias"
            "matched_official_alias": "",
        }

        # -------- 1) exact (main + aliases) --------
        exact_hit_key: Optional[str] = None
        exact_hit_via: str = ""
        exact_hit_alias: str = ""

        if ok_main in key_to_idxs:
            exact_hit_key = ok_main
            exact_hit_via = "official_name"
        else:
            for a_raw, ak in alias_pairs:
                if ak and ak in key_to_idxs:
                    exact_hit_key = ak
                    exact_hit_via = "official_alias"
                    exact_hit_alias = a_raw
                    break

        if exact_hit_key is not None:
            idxs = key_to_idxs[exact_hit_key]
            matched_agg_idx.update(idxs)

            out["match"]["found_in_aggregators"] = True
            out["match"]["match_type"] = "exact"
            out["match"]["match_score"] = 100.0

            names = [agg_names[i] for i in idxs]
            out["match"]["matched_name"] = sorted(names, key=len)[0]
            out["match"]["aggregator_sources"] = sorted({union[i]["source"] for i in idxs})

            out["match"]["matched_via"] = exact_hit_via
            if exact_hit_via == "official_alias":
                out["match"]["matched_official_alias"] = exact_hit_alias

            exact_matches += 1

        else:
            # -------- 2) fuzzy (main + aliases), затем guard --------
            best_key_main, score_main = best_fuzzy(ok_main, choices)

            best_score = score_main
            best_key = best_key_main
            best_via = "official_name"
            best_alias_raw = ""

            for a_raw, ak in alias_pairs:
                if not ak:
                    continue
                k2, s2 = best_fuzzy(ak, choices)
                if s2 > best_score:
                    best_score = s2
                    best_key = k2
                    best_via = "official_alias"
                    best_alias_raw = a_raw

            guard_query = ok_main if best_via == "official_name" else official_key(best_alias_raw)

            passed = (best_score >= 90 and best_key and _passes_fuzzy_guard(guard_query, best_key, best_score))
            if passed:
                idxs = key_to_idxs.get(best_key, [])
                if idxs:
                    matched_agg_idx.update(idxs)

                out["match"]["found_in_aggregators"] = True
                out["match"]["match_type"] = "fuzzy"
                out["match"]["match_score"] = float(best_score)
                out["match"]["matched_name"] = agg_names[key_to_first_idx[best_key]]
                out["match"]["aggregator_sources"] = sorted({union[i]["source"] for i in idxs}) if idxs else []

                out["match"]["matched_via"] = best_via
                if best_via == "official_alias":
                    out["match"]["matched_official_alias"] = best_alias_raw

                fuzzy_matches += 1

                if best_score < 96:
                    out["match"]["review"] = True
            else:
                if best_score >= 90 and best_key:
                    q_sig = _significant_tokens(guard_query)
                    b_sig = _significant_tokens(best_key)
                    fuzzy_rejected.append({
                        "org": org,
                        "guard_query": guard_query,
                        "best_key": best_key,
                        "best_score": float(best_score),
                        "best_via": best_via,
                        "best_alias_raw": best_alias_raw,
                        "q_sig": sorted(q_sig),
                        "b_sig": sorted(b_sig),
                        "intersection": sorted(q_sig & b_sig),
                    })
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

    # unmatched aggregators (в union)
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
    save_json(OUT_FUZZY_REJECTED, fuzzy_rejected)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    # --- debug: near-misses (optional) ---
    near = []
    for r in enriched:
        m = r.get("match", {})
        if m.get("match_type") == "none":
            # попробуем оценить лучший fuzzy по ok_main только для диагностики
            org = r.get("organization_name","") or ""
            ok = official_key(org)
            # choices есть в main()
            bk, sc = best_fuzzy(ok, choices)
            if sc >= 75:
                near.append((sc, org, ok, bk))

    near.sort(reverse=True, key=lambda t: t[0])
    print("\nTOP NEAR MISSES (score>=75, unmatched):", len(near))
    for sc, org, ok, bk in near[:20]:
        print(f"- {sc:.1f} | org={org[:70]} | ok={ok[:60]} | best={bk[:60]}")



if __name__ == "__main__":
    main()
