#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

# ---------- helpers ----------

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_jsonl(path: str, items: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for obj in items:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def json_stream_parse(text: str) -> List[Any]:
    """
    Robustly parse:
    - a single JSON object/array, OR
    - multiple JSON objects concatenated one after another
    Uses json.JSONDecoder.raw_decode to iterate.
    """
    dec = json.JSONDecoder()
    i = 0
    n = len(text)
    out = []
    while True:
        # skip whitespace
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        try:
            obj, j = dec.raw_decode(text, i)
        except json.JSONDecodeError as e:
            # Provide a short context for debugging
            ctx = text[i:i+200].replace("\n", "\\n")
            raise RuntimeError(f"JSON decode failed at pos {i}: {e}. Context: {ctx}") from e
        out.append(obj)
        i = j
    return out

def normalize_ws(s: str) -> str:
    # normalize spaces and quotes for stable matching/search
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("«", '"').replace("»", '"')
    return s

def norm_org_name(name: str) -> str:
    name = normalize_ws(name).lower()
    # very light normalization only (no loss): keep original too
    # unify common variants
    name = name.replace("федеральное государственное бюджетное образовательное учреждение высшего образования", "фгбоу во")
    name = name.replace("автономная некоммерческая образовательная организация высшего образования", "ано во")
    name = name.replace("образовательное учреждение высшего образования", "во")
    return name

def is_university_candidate(name: str) -> bool:
    n = name.lower()
    return (
        "высшего образования" in n
        or "университет" in n
        or "институт" in n
        or "академ" in n  # академия/академический
        or "фгбоу во" in n
        or "во " in n
    )

# ---------- main extraction logic ----------

def extract_pages_and_records(objs: List[Any]) -> Tuple[List[dict], List[dict]]:
    """
    pages: list of top-level page objects (full objects, lossless)
    records: flattened items from pages[*]["data"] if present, else if obj is list of records etc
    """
    pages: List[dict] = []
    records: List[dict] = []

    # case 1: file is a single JSON array
    if len(objs) == 1 and isinstance(objs[0], list):
        arr = objs[0]
        # If it's a list of page objects with "data"
        if all(isinstance(x, dict) and "data" in x for x in arr):
            for p in arr:
                pages.append(p)
                if isinstance(p.get("data"), list):
                    records.extend(p["data"])
        # If it's directly a list of records
        elif all(isinstance(x, dict) and "organization_name" in x for x in arr):
            # no pages, just records
            records = arr
        else:
            # mixed/unknown array -> store as a single "page-like" wrapper to be lossless
            pages.append({"_wrapper": "array_root", "data_root": arr})
    else:
        # case 2: multiple top-level JSON objects concatenated
        for obj in objs:
            if isinstance(obj, dict) and "data" in obj and isinstance(obj.get("data"), list):
                pages.append(obj)
                records.extend(obj["data"])
            elif isinstance(obj, list) and all(isinstance(x, dict) for x in obj):
                # ambiguous list: treat as "records batch" but keep lossless page wrapper too
                pages.append({"_wrapper": "array_block", "data_root": obj})
                # if it looks like records, extend
                if any("organization_name" in x for x in obj):
                    records.extend(obj)
            else:
                # unknown object -> keep lossless
                pages.append({"_wrapper": "unknown_block", "data_root": obj})

    return pages, records

def normalize_records(records: List[dict]) -> List[dict]:
    out = []
    for r in records:
        name = r.get("organization_name")
        if not isinstance(name, str):
            name = ""
        obj = dict(r)  # no loss: copy all original keys
        obj["_norm"] = {
            "organization_name_ws": normalize_ws(name),
            "organization_name_norm": norm_org_name(name),
            "is_university_candidate": bool(name and is_university_candidate(name)),
        }
        out.append(obj)
    return out

def dedup_universities(records_norm: List[dict]) -> Tuple[List[dict], List[dict]]:
    """
    Lossless dedup:
    - We DO NOT delete originals. We create:
      * universities.unique.json: representative per group
      * universities.unique.audit.jsonl: mapping group -> all source records
    Group key heuristic (can be tuned later):
    - normalized org name + (optional) OGRN/INN if present
    """
    groups: Dict[str, List[dict]] = {}

    for r in records_norm:
        if not r.get("_norm", {}).get("is_university_candidate"):
            continue
        name_norm = r["_norm"]["organization_name_norm"]
        inn = (r.get("inn") or r.get("organization", {}).get("inn") or "")
        ogrn = (r.get("ogrn") or r.get("organization", {}).get("ogrn") or "")
        # many dumps don't have inn/ogrn at record level; keep in key if present
        key = f"{name_norm}||inn={inn}||ogrn={ogrn}"
        groups.setdefault(key, []).append(r)

    # choose representative: keep the one with latest date_issue if parseable, else first
    def parse_ddmmyyyy(s: str) -> Optional[Tuple[int,int,int]]:
        if not isinstance(s, str):
            return None
        m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s.strip())
        if not m:
            return None
        dd, mm, yyyy = map(int, m.groups())
        return (yyyy, mm, dd)

    unique = []
    audit = []
    for key, items in groups.items():
        best = items[0]
        best_dt = parse_ddmmyyyy(best.get("date_issue","")) or (0,0,0)
        for it in items[1:]:
            dt = parse_ddmmyyyy(it.get("date_issue","")) or (0,0,0)
            if dt > best_dt:
                best = it
                best_dt = dt
        rep = {
            "group_key": key,
            "organization_name": best.get("organization_name"),
            "register_number": best.get("register_number"),
            "date_issue": best.get("date_issue"),
            "date_end": best.get("date_end"),
            "id": best.get("id"),
            "sources_count": len(items),
        }
        unique.append(rep)
        audit.append({
            "group_key": key,
            "representative_id": best.get("id"),
            "sources": items,  # lossless: keep all source records
        })

    return unique, audit

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to data/raw/uni.json")
    ap.add_argument("--outdir", required=True, help="output directory, e.g. data/processed")
    args = ap.parse_args()

    ensure_dir(args.outdir)
    raw = read_text(args.input)

    objs = json_stream_parse(raw)
    pages, records = extract_pages_and_records(objs)
    records_norm = normalize_records(records)

    # write lossless outputs
    write_jsonl(os.path.join(args.outdir, "pages.extracted.jsonl"), pages)
    write_jsonl(os.path.join(args.outdir, "records.all.jsonl"), records)
    write_jsonl(os.path.join(args.outdir, "records.all.normalized.jsonl"), records_norm)

    # university layer
    uni_candidates = [r for r in records_norm if r.get("_norm", {}).get("is_university_candidate")]
    write_jsonl(os.path.join(args.outdir, "universities.candidates.jsonl"), uni_candidates)

    unique, audit = dedup_universities(records_norm)
    write_json(os.path.join(args.outdir, "universities.unique.json"), unique)
    write_jsonl(os.path.join(args.outdir, "universities.unique.audit.jsonl"), audit)

    # quick stats
    stats = {
        "pages_count": len(pages),
        "records_count": len(records),
        "uni_candidates_count": len(uni_candidates),
        "uni_unique_count": len(unique),
    }
    write_json(os.path.join(args.outdir, "stats.json"), stats)
    print(json.dumps(stats, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
