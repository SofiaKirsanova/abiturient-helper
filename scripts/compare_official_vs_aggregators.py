#!/usr/bin/env python3
from __future__ import annotations
import json, re, glob
from rapidfuzz import fuzz, process

def norm(s: str) -> str:
    s = s.lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("«", '"').replace("»", '"')
    # мягкие сокращения
    s = s.replace("федеральное государственное автономное образовательное учреждение высшего образования", "фгаоу во")
    s = s.replace("федеральное государственное бюджетное образовательное учреждение высшего образования", "фгбоу во")
    return s

official_path = "data/processed/universities.unique.json"
with open(official_path, encoding="utf-8") as f:
    official = json.load(f)

official_names = [x["organization_name"] for x in official if x.get("organization_name")]
official_norm = {norm(n): n for n in official_names}
official_keys = list(official_norm.keys())

agg_files = sorted(glob.glob("data/processed/aggregators/*.names.json"))

report = {}
THRESH = 90  # можно опустить до 85, если будет много пропусков из-за вариантов написания

for af in agg_files:
    with open(af, encoding="utf-8") as f:
        names = json.load(f)
    missing = []
    matched = []
    for n in names:
        nk = norm(n)
        hit = process.extractOne(nk, official_keys, scorer=fuzz.token_set_ratio)
        if not hit or hit[1] < THRESH:
            missing.append(n)
        else:
            matched.append({"agg": n, "official": official_norm[hit[0]], "score": hit[1]})
    report[af] = {
        "agg_total": len(names),
        "matched": len(matched),
        "missing": len(missing),
        "missing_examples": missing[:50],
    }

with open("data/processed/aggregators/compare_report.json", "w", encoding="utf-8") as w:
    json.dump(report, w, ensure_ascii=False, indent=2)

print(json.dumps(report, ensure_ascii=False, indent=2))
