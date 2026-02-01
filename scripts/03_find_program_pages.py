#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
import time
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple, Set
from urllib.parse import urlparse, urljoin, urldefrag

import requests
from bs4 import BeautifulSoup
import tldextract


UA = "Mozilla/5.0 (X11; Linux x86_64) abiturient-helper/0.1 (+https://example.invalid)"
TIMEOUT = 25

# High-signal keywords (score boosts)
KW_STRONG = [
    "абитуриент", "поступающ", "поступление", "прием", "приём",
    "бакалавриат", "специалитет", "образовательные программы",
    "направления подготовки", "программы обучения", "образование",
    "программы", "учебные планы",
]

# Medium signal keywords
KW_MED = [
    "education", "admission", "entrant", "enrol", "enroll",
    "bachelor", "specialist", "program", "programmes",
]

# Penalize pages that are likely irrelevant
KW_BAD = [
    "новости", "news", "press", "media", "контакты", "contact",
    "ваканс", "jobs", "career", "science", "research",
    "студент", "student", "library", "библиотек", "conference",
    "login", "signin", "auth", "profile",
    "privacy", "cookie", "map", "site-map", "sitemap",
]


def norm_text(s: str) -> str:
    s = s.lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def same_site(base: str, url: str) -> bool:
    b = urlparse(base)
    u = urlparse(url)
    return (b.scheme, b.netloc) == (u.scheme, u.netloc)


def canonicalize_url(url: str) -> str:
    url, _frag = urldefrag(url)
    # remove obvious tracking
    url = re.sub(r"(\?|&)(utm_[^=]+=[^&]+)", r"\1", url)
    url = url.replace("?&", "?").rstrip("?&")
    return url


def fetch(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ctype and "application/xhtml" not in ctype:
            # still allow if looks like HTML
            if "<html" not in (r.text or "").lower():
                return None
        return r.text
    except Exception:
        return None


def extract_links(html: str, base_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        href = href.strip()
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        abs_url = urljoin(base_url, href)
        abs_url = canonicalize_url(abs_url)
        text = a.get_text(" ", strip=True)[:200]
        out.append((abs_url, text))
    return out


def score_link(url: str, anchor_text: str) -> float:
    u = norm_text(url)
    t = norm_text(anchor_text)

    score = 0.0

    # Strong keywords in URL/title
    for kw in KW_STRONG:
        k = norm_text(kw)
        if k in u:
            score += 12.0
        if k in t:
            score += 10.0

    for kw in KW_MED:
        k = norm_text(kw)
        if k in u:
            score += 5.0
        if k in t:
            score += 4.0

    for kw in KW_BAD:
        k = norm_text(kw)
        if k in u:
            score -= 6.0
        if k in t:
            score -= 5.0

    # Prefer shorter paths (not always, but helps)
    path = urlparse(url).path
    depth = path.count("/")
    score -= 0.4 * max(0, depth - 1)

    # PDF sometimes contains lists; keep but lower priority
    if u.endswith(".pdf"):
        score -= 2.0

    return score


def get_root(site: str) -> str:
    p = urlparse(site)
    return f"{p.scheme}://{p.netloc}"


def pick_top_candidates(links: List[Tuple[str, str]], site_root: str, top_k: int = 30) -> List[Dict[str, Any]]:
    # keep only same site
    filtered = []
    for (u, txt) in links:
        if not same_site(site_root, u):
            continue
        filtered.append((u, txt, score_link(u, txt)))

    # dedupe by url keep max score
    best: Dict[str, Dict[str, Any]] = {}
    for u, txt, sc in filtered:
        if u not in best or sc > best[u]["score"]:
            best[u] = {"url": u, "text": txt, "score": sc}

    cands = list(best.values())
    cands.sort(key=lambda x: x["score"], reverse=True)
    return cands[:top_k]


def bfs_find_program_pages(site_root: str, seeds: List[str], max_pages: int, max_depth: int, sleep_s: float) -> List[Dict[str, Any]]:
    """
    Crawl limited subset of pages, prioritize by keyword-scored links.
    Returns ranked candidate pages (url + score + reason snippets).
    """
    visited: Set[str] = set()
    frontier: List[Tuple[str, int]] = [(s, 0) for s in seeds]
    candidates: Dict[str, float] = {}  # url -> best score

    while frontier and len(visited) < max_pages:
        url, depth = frontier.pop(0)
        url = canonicalize_url(url)
        if url in visited:
            continue
        visited.add(url)

        html = fetch(url)
        time.sleep(sleep_s)
        if not html:
            continue

        # Score the page itself based on URL path and the page title
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        page_score = score_link(url, title)

        if page_score > candidates.get(url, float("-inf")):
            candidates[url] = page_score

        if depth >= max_depth:
            continue

        links = extract_links(html, url)
        ranked = pick_top_candidates(links, site_root, top_k=25)

        # Push higher-score links first
        for item in ranked:
            nxt = item["url"]
            if nxt in visited:
                continue
            # Only expand decent links (avoid wandering)
            if item["score"] >= 6.0:
                frontier.append((nxt, depth + 1))

        # Keep frontier ordered a bit by depth then URL (light)
        frontier.sort(key=lambda x: (x[1], x[0]))

    # convert candidates dict to list
    out = [{"url": u, "score": round(sc, 3)} for u, sc in candidates.items()]
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sites_jsonl", required=True, help="universities_sites.jsonl")
    ap.add_argument("--out", required=True, help="universities_program_pages.jsonl")
    ap.add_argument("--sleep", type=float, default=1.0, help="delay between requests")
    ap.add_argument("--limit", type=int, default=0, help="process first N only")
    ap.add_argument("--max_pages", type=int, default=18, help="max pages to fetch per site")
    ap.add_argument("--max_depth", type=int, default=2, help="BFS depth from seeds")
    args = ap.parse_args()

    rows = read_jsonl(args.sites_jsonl)
    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    out_rows = []
    for i, row in enumerate(rows, 1):
        univ = row.get("university")
        site = row.get("site")
        conf = row.get("confidence", 0.0)

        result = {
            "university": univ,
            "site": site,
            "site_confidence": conf,
            "program_pages": [],
            "candidates": [],
            "status": "no_site" if not site else "ok",
        }

        if not site:
            out_rows.append(result)
            print(f"[{i}/{len(rows)}] no site :: {univ}")
            continue

        site_root = get_root(site)
        home = site_root + "/"

        # seeds: home + a couple of common paths
        seeds = [home]
        # If site already has a path (not root), keep it too
        if site != site_root:
            seeds.append(site)

        # Try direct well-known paths (cheap guesses)
        for guess in ["/abiturient/", "/abitur/", "/entrant/", "/admissions/", "/education/"]:
            seeds.append(urljoin(site_root, guess))

        # BFS limited crawl
        candidates = bfs_find_program_pages(
            site_root=site_root,
            seeds=seeds,
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            sleep_s=args.sleep,
        )

        # Keep best candidates; program_pages = top ones over threshold
        result["candidates"] = candidates[:25]
        program_pages = [c["url"] for c in candidates if c["score"] >= 18.0][:8]

        # If nothing above threshold, relax
        if not program_pages:
            program_pages = [c["url"] for c in candidates if c["score"] >= 12.0][:6]

        result["program_pages"] = program_pages
        if not program_pages:
            result["status"] = "not_found"
        else:
            result["status"] = "ok"

        out_rows.append(result)
        print(f"[{i}/{len(rows)}] pages={len(program_pages)} status={result['status']} :: {univ}")

    write_jsonl(args.out, out_rows)
    print(f"[OK] wrote: {args.out}")


if __name__ == "__main__":
    main()
