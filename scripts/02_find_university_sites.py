#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup
import tldextract
from rapidfuzz import fuzz


UA = "Mozilla/5.0 (X11; Linux x86_64) abiturient-helper/0.1 (+https://example.invalid)"
TIMEOUT = 20


def norm_univ_name(name: str) -> str:
    s = name.lower().strip()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)

    # remove some high-noise phrases (keep conservative)
    s = re.sub(r"\(.*?\)", "", s).strip()
    s = re.sub(r"\s+", " ", s)

    # shorten "имени ..." fragments for search robustness (optional)
    s = re.sub(r"\s+имени\s+.+$", "", s).strip()
    return s


def is_plausible_official_site(url: str) -> bool:
    """
    Heuristics: avoid social media and directories.
    """
    bad_hosts = (
        "vk.com", "ok.ru", "t.me", "telegram", "youtube.com",
        "rutube.ru", "dzen.ru", "yandex.ru", "2gis.ru",
        "tabiturient.ru", "postupi.online", "ucheba.ru",
        "wikipedia.org"
    )
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    if any(b in host for b in bad_hosts):
        return False
    return True


def canonicalize_root(url: str) -> str:
    """
    Keep scheme + netloc only.
    """
    p = urlparse(url)
    scheme = p.scheme or "https"
    netloc = p.netloc
    if not netloc:
        return url
    return f"{scheme}://{netloc}"


def score_candidate(univ: str, url: str, title: str = "") -> float:
    """
    Combined heuristic score to pick best site among candidates.
    """
    u_norm = norm_univ_name(univ)
    title_norm = (title or "").lower().replace("ё", "е")
    url_norm = url.lower()

    # domain signal
    ext = tldextract.extract(url)
    dom = f"{ext.domain}.{ext.suffix}".lower()

    base = 0.0
    # prefer .ru / .edu.ru / .org / .su a bit
    if dom.endswith(".ru") or dom.endswith(".su"):
        base += 5.0
    if dom.endswith(".edu.ru"):
        base += 6.0

    # prefer if university tokens appear in title or URL
    base += 0.10 * fuzz.token_set_ratio(u_norm, title_norm)
    base += 0.06 * fuzz.token_set_ratio(u_norm, url_norm)

    # penalize obvious non-official
    if not is_plausible_official_site(url):
        base -= 50.0

    return base


def fetch_json(url: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def wiki_official_site(univ_name: str) -> Optional[str]:
    """
    Strategy: use Wikipedia REST search, then follow page summary (can include official website).
    This is best-effort (not all org pages have it).
    """
    q = univ_name
    search_url = f"https://ru.wikipedia.org/w/rest.php/v1/search/title?q={quote_plus(q)}&limit=5"
    data = fetch_json(search_url)
    if not data or "pages" not in data:
        return None

    # pick the most similar title
    best = None
    best_sim = -1
    for p in data["pages"]:
        title = p.get("title", "")
        sim = fuzz.token_set_ratio(norm_univ_name(univ_name), norm_univ_name(title))
        if sim > best_sim:
            best_sim = sim
            best = p

    if not best or best_sim < 60:
        return None

    # summary endpoint sometimes has "content_urls" but not official site;
    # still, we can open the HTML and try to extract "Официальный сайт" from infobox (last resort)
    page_title = best.get("title")
    if not page_title:
        return None

    html_url = f"https://ru.wikipedia.org/wiki/{quote_plus(page_title.replace(' ', '_'))}"
    try:
        r = requests.get(html_url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        # Typical: in infobox, row with label "Сайт" or "Официальный сайт"
        infobox = soup.select_one("table.infobox")
        if not infobox:
            return None
        for tr in infobox.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(" ", strip=True).lower()
            if "сайт" in label:
                a = td.select_one("a[href^='http']")
                if a and a.get("href"):
                    return canonicalize_root(a["href"])
    except Exception:
        return None

    return None


def ddg_search(univ_name: str, max_results: int = 8) -> List[Tuple[str, str]]:
    """
    DuckDuckGo HTML (no API key). Returns list of (url, title).
    Note: DDG may rate-limit; we keep delays and conservative requests.
    """
    query = f"{univ_name} официальный сайт"
    url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out = []
        for res in soup.select(".result"):
            a = res.select_one("a.result__a")
            if not a:
                continue
            href = a.get("href")
            title = a.get_text(" ", strip=True)
            if not href:
                continue
            out.append((href, title))
            if len(out) >= max_results:
                break
        return out
    except Exception:
        return []


@dataclass
class SiteResult:
    university: str
    site: Optional[str]
    method: str
    confidence: float
    candidates: List[Dict[str, Any]]


def find_site_for_university(univ: str, sleep_s: float = 1.0) -> SiteResult:
    candidates: List[Dict[str, Any]] = []

    # 1) Wikipedia
    wsite = wiki_official_site(univ)
    if wsite:
        return SiteResult(
            university=univ,
            site=wsite,
            method="wikipedia_infobox",
            confidence=0.95,
            candidates=[{"url": wsite, "title": "from wikipedia", "score": 999}],
        )

    time.sleep(sleep_s)

    # 2) DuckDuckGo candidates
    ddg = ddg_search(univ)
    for (u, title) in ddg:
        score = score_candidate(univ, u, title=title)
        candidates.append({"url": u, "title": title, "score": score})

    candidates.sort(key=lambda x: x["score"], reverse=True)

    if not candidates:
        return SiteResult(university=univ, site=None, method="none", confidence=0.0, candidates=[])

    best = candidates[0]
    best_url = best["url"]
    best_score = best["score"]

    # confidence heuristic
    conf = 0.0
    if best_score >= 40:
        conf = 0.90
    elif best_score >= 30:
        conf = 0.75
    elif best_score >= 20:
        conf = 0.55
    else:
        conf = 0.35

    # ambiguous if top-2 too close
    if len(candidates) >= 2:
        if candidates[1]["score"] >= best_score - 2.0:
            conf = min(conf, 0.60)

    # canonicalize
    best_root = canonicalize_root(best_url)

    return SiteResult(
        university=univ,
        site=best_root if conf >= 0.55 else None,
        method="duckduckgo_html",
        confidence=conf,
        candidates=candidates[:5],
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universities", required=True, help="Path to full_universities.json")
    ap.add_argument("--out", required=True, help="Where to write universities_sites.jsonl")
    ap.add_argument("--sleep", type=float, default=1.2, help="Delay between queries (seconds)")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N (0 = all)")
    args = ap.parse_args()

    universities = json.loads(open(args.universities, "r", encoding="utf-8").read())
    if args.limit and args.limit > 0:
        universities = universities[: args.limit]

    out_path = args.out
    with open(out_path, "w", encoding="utf-8") as f:
        for i, univ in enumerate(universities, 1):
            res = find_site_for_university(univ, sleep_s=args.sleep)
            obj = {
                "university": res.university,
                "site": res.site,
                "method": res.method,
                "confidence": round(res.confidence, 3),
                "candidates": res.candidates,
            }
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            print(f"[{i}/{len(universities)}] conf={obj['confidence']:.2f} site={obj['site']} :: {univ}")

    print(f"[OK] wrote: {out_path}")


if __name__ == "__main__":
    main()
