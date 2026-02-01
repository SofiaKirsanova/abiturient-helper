#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Any


def norm_text(s: str) -> str:
    s = s.lower().strip()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


def build_index(merged_sets: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Output schema:
    {
      "09.03.01": {
        "direction_code": "...",
        "title": "...",
        "level": "bach|spec",
        "ug_code": "...",
        "ug_title": "...",
        "aliases": ["...", ...],
        "aliases_norm": ["...", ...]
      },
      ...
    }
    """
    out: Dict[str, Any] = {}

    for ug in merged_sets:
        ug_code = ug.get("ug_code")
        ug_title = ug.get("ug_title")
        for d in ug.get("directions", []):
            code = d["direction_code"]
            title = d["title"]
            level = d.get("level")

            aliases = []
            aliases.append(title)
            aliases.extend(d.get("tabiturient_titles", []) or [])

            # Basic alias expansions (cheap but useful)
            # 1) remove quotes
            aliases += [re.sub(r"[\"«»]", "", a) for a in aliases]
            # 2) replace "и" with "&" (rare but sometimes on sites)
            aliases += [a.replace(" и ", " & ") for a in aliases]
            # 3) squeeze spaces
            aliases = [re.sub(r"\s+", " ", a).strip() for a in aliases]

            # dedupe while preserving order
            seen = set()
            aliases_dedup = []
            for a in aliases:
                if a and a not in seen:
                    seen.add(a)
                    aliases_dedup.append(a)

            out[code] = {
                "direction_code": code,
                "title": title,
                "level": level,
                "ug_code": ug_code,
                "ug_title": ug_title,
                "aliases": aliases_dedup,
                "aliases_norm": [norm_text(a) for a in aliases_dedup],
            }

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--merged", required=True, help="Path to merged_moscow_sets.json")
    ap.add_argument("--out", required=True, help="Path to write directions_index.json")
    args = ap.parse_args()

    merged_path = Path(args.merged)
    out_path = Path(args.out)

    merged_sets = json.loads(merged_path.read_text(encoding="utf-8"))
    idx = build_index(merged_sets)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] directions indexed: {len(idx)}")
    print(f"[OK] wrote: {out_path}")


if __name__ == "__main__":
    main()
