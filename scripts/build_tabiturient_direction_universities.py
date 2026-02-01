"""
build_tabiturient_direction_universities.py

Зачем:
- Полуавтоматически сопоставить направления (merged_moscow_sets.json)
  с вузами, где они есть, используя страницы tabiturient по каждому направлению.
- Дальше матчить найденные вузы с каноническим списком (full_universities.json).

Выход:
- data/processed/tabiturient_direction_unis.json
  (направление -> список вузов как на tabiturient)
- data/processed/university_directions.json
  (канонический вуз -> список направлений)
- data/processed/university_directions_report.json
  (статистика + проблемные совпадения)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

MERGED_PATH = "data/processed/merged_moscow_sets.json"
UNIS_PATH = "data/processed/full_universities.json"

RAW_DIR = "data/raw/tabiturient_directions"
OUT_RAW = "data/processed/tabiturient_direction_unis.json"
OUT_MATCHED = "data/processed/university_directions.json"
OUT_REPORT = "data/processed/university_directions_report.json"

BAD_PHRASES = {
    "вход",
    "регистрация",
    "поиск",
    "калькулятор",
    "отзывы",
    "рейтинги",
    "сравнить",
    "дни открытых дверей",
    "статьи",
    "вопросы",
    "сохраненные программы",
    "премиум-доступ",
    "общий рейтинг вузов",
    "топ-100 популярных образовательных программ",
}

STOPWORDS = {
    "имени",
    "им",
    "государственный",
    "государственная",
    "государственное",
    "федеральный",
    "федеральное",
    "федеральная",
    "национальный",
    "национальное",
    "национальная",
    "исследовательский",
    "исследовательское",
    "исследовательская",
    "московский",
    "московская",
    "московское",
    "российский",
    "российская",
    "российское",
    "университет",
    "академия",
    "институт",
    "университета",
    "академии",
    "института",
}


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def is_valid_name(name: str) -> bool:
    if not name:
        return False
    low = name.lower()
    if any(bad in low for bad in BAD_PHRASES):
        return False
    return any(ch.isalpha() for ch in name)


def cache_path_for_url(url: str) -> str:
    parsed = urlparse(url)
    slug = parsed.path.strip("/").split("/")[-1] or "index"
    return os.path.join(RAW_DIR, f"{slug}.html")


def fetch(url: str, force: bool) -> str:
    os.makedirs(RAW_DIR, exist_ok=True)
    cache_path = cache_path_for_url(url)
    if os.path.exists(cache_path) and not force:
        with open(cache_path, "r", encoding="utf-8") as f:
            return f.read()
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    resp.raise_for_status()
    html = resp.text
    with open(cache_path, "w", encoding="utf-8") as f:
        f.write(html)
    return html


def parse_universities(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    results = set()

    for sp in soup.find_all("span", class_="font2"):
        text = clean(sp.get_text(" ", strip=True))
        if "|" not in text:
            continue
        next_span = sp.find_next("span", class_="font11")
        if not next_span:
            continue
        name = clean(next_span.get_text(" ", strip=True))
        if is_valid_name(name):
            results.add(name)

    if results:
        return sorted(results)

    # fallback: regex on raw html
    pattern = re.compile(
        r'class="font2"[^>]*>[^<]*\|\s*</span>\s*<span[^>]*class="font11"[^>]*>([^<]+)</span>',
        re.IGNORECASE,
    )
    for m in pattern.finditer(html):
        name = clean(m.group(1))
        if is_valid_name(name):
            results.add(name)

    return sorted(results)


def normalize_tokens(name: str) -> List[str]:
    text = name.lower().replace("ё", "е")
    text = re.sub(r"\(.*?\)", " ", text)
    text = re.sub(r"[^a-zа-я0-9]+", " ", text)
    tokens = [t for t in text.split() if t and t not in STOPWORDS]
    return tokens


def similarity(a: str, b: str) -> float:
    a_tokens = set(normalize_tokens(a))
    b_tokens = set(normalize_tokens(b))
    jaccard = 0.0
    if a_tokens and b_tokens:
        jaccard = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)
    ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
    return max(jaccard, ratio)


@dataclass
class MatchResult:
    best: Optional[str]
    best_score: float
    second: Optional[str]
    second_score: float


def best_match(name: str, candidates: Sequence[str]) -> MatchResult:
    scored = []
    for c in candidates:
        scored.append((similarity(name, c), c))
    scored.sort(reverse=True, key=lambda x: x[0])
    best_score, best = scored[0] if scored else (0.0, None)
    second_score, second = scored[1] if len(scored) > 1 else (0.0, None)
    return MatchResult(best=best, best_score=best_score, second=second, second_score=second_score)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=float, default=0.3, help="Пауза между запросами")
    parser.add_argument("--force", action="store_true", help="Перекачать HTML, игнорируя кеш")
    parser.add_argument("--limit", type=int, default=None, help="Ограничить число направлений")
    args = parser.parse_args()

    merged = load_json(MERGED_PATH)
    canonical_unis = load_json(UNIS_PATH)

    directions = []
    for g in merged:
        for d in g.get("directions", []):
            directions.append(d)

    if args.limit:
        directions = directions[: args.limit]

    raw_by_dir: Dict[str, Dict[str, object]] = {}
    all_raw_unis: Dict[str, int] = {}

    for idx, d in enumerate(directions, start=1):
        code = d["direction_code"]
        urls = d.get("urls") or []
        collected = set()
        for url in urls:
            try:
                html = fetch(url, force=args.force)
            except Exception as e:
                print(f"[WARN] {code}: {url} -> {e}")
                continue
            collected.update(parse_universities(html))
            time.sleep(args.sleep)

        raw_by_dir[code] = {
            "direction_code": code,
            "title": d.get("title"),
            "level": d.get("level"),
            "urls": urls,
            "universities": sorted(collected),
        }
        for u in collected:
            all_raw_unis[u] = all_raw_unis.get(u, 0) + 1

        print(f"[{idx}/{len(directions)}] {code}: {len(collected)} вузов")

    with open(OUT_RAW, "w", encoding="utf-8") as f:
        json.dump(raw_by_dir, f, ensure_ascii=False, indent=2)

    # matching raw -> canonical
    name_map: Dict[str, Optional[str]] = {}
    ambiguous: Dict[str, Dict[str, object]] = {}
    unmatched: Dict[str, Dict[str, object]] = {}

    for raw_name in all_raw_unis:
        match = best_match(raw_name, canonical_unis)
        if not match.best or match.best_score < 0.72:
            name_map[raw_name] = None
            unmatched[raw_name] = {
                "best": match.best,
                "best_score": match.best_score,
                "second": match.second,
                "second_score": match.second_score,
            }
            continue

        if match.second and (match.best_score - match.second_score) < 0.05 and match.best_score < 0.9:
            name_map[raw_name] = None
            ambiguous[raw_name] = {
                "best": match.best,
                "best_score": match.best_score,
                "second": match.second,
                "second_score": match.second_score,
            }
            continue

        name_map[raw_name] = match.best

    # build canonical -> directions
    uni_to_dirs: Dict[str, Dict[str, object]] = {u: {"university": u, "directions": []} for u in canonical_unis}

    for code, payload in raw_by_dir.items():
        for raw_uni in payload.get("universities", []):
            canonical = name_map.get(raw_uni)
            if not canonical:
                continue
            entry = uni_to_dirs[canonical]
            entry["directions"].append(
                {
                    "direction_code": code,
                    "title": payload.get("title"),
                    "level": payload.get("level"),
                }
            )

    # sort directions in each university
    for entry in uni_to_dirs.values():
        entry["directions"].sort(key=lambda x: x["direction_code"])

    with open(OUT_MATCHED, "w", encoding="utf-8") as f:
        json.dump(uni_to_dirs, f, ensure_ascii=False, indent=2)

    report = {
        "directions_total": len(raw_by_dir),
        "raw_universities_total": len(all_raw_unis),
        "matched_universities": sum(1 for v in name_map.values() if v),
        "ambiguous_universities": len(ambiguous),
        "unmatched_universities": len(unmatched),
        "ambiguous": ambiguous,
        "unmatched": unmatched,
    }

    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("Saved:")
    print(" -", OUT_RAW)
    print(" -", OUT_MATCHED)
    print(" -", OUT_REPORT)


if __name__ == "__main__":
    main()



