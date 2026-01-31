"""
merge_okso_with_tabiturient.py

Зачем:
- Свести (join) официальный перечень направлений (okso_1061.json) с тем,
  что найдено на tabiturient по наборам ЕГЭ для Москва/МО.
- Сделать удобный для сайта JSON: укрупнённые группы -> направления,
  и для каждого направления указать, под какие наборы ЕГЭ оно подходит.

Вход:
- data/processed/okso_1061.json
- data/processed/tabiturient_sets.json

Выход:
- data/processed/merged_moscow_sets.json
- data/processed/merge_report.json (статистика: сколько сматчилось/не сматчилось)
"""

import json
from collections import defaultdict

OKSO_PATH = "data/processed/okso_1061.json"
TABI_PATH = "data/processed/tabiturient_sets.json"

OUT_MERGED = "data/processed/merged_moscow_sets.json"
OUT_REPORT = "data/processed/merge_report.json"


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    okso = load_json(OKSO_PATH)
    tabi = load_json(TABI_PATH)

    # индекс ОКСО по коду направления
    okso_by_code = {r["code"]: r for r in okso}

    # агрегируем tabiturient по коду направления: какие наборы ЕГЭ, какие ссылки
    by_dir = {}
    for r in tabi:
        code = r["direction_code"]
        entry = by_dir.setdefault(
            code,
            {
                "direction_code": code,
                "tabiturient_titles": set(),
                "sets": set(),
                "urls": set(),
                "tabi_ug_codes": set(),
                "tabi_ug_titles": set(),
            },
        )
        entry["sets"].add(r["set_key"])
        if r.get("direction_title"):
            entry["tabiturient_titles"].add(r["direction_title"])
        if r.get("url"):
            entry["urls"].add(r["url"])
        if r.get("ug_code"):
            entry["tabi_ug_codes"].add(r["ug_code"])
        if r.get("ug_title"):
            entry["tabi_ug_titles"].add(r["ug_title"])

    matched = 0
    unmatched = 0

    # строим структуру по укрупнённым группам (берём из ОКСО как "каноническое")
    groups = defaultdict(lambda: {"ug_code": None, "ug_title": None, "directions": []})

    for code, info in by_dir.items():
        ok = okso_by_code.get(code)

        if ok:
            matched += 1
            ug_code = ok.get("ug_code")
            ug_title = ok.get("ug_title")
            level = ok.get("level")
            canonical_title = ok.get("title")
        else:
            unmatched += 1
            # fallback: используем то, что есть в tabiturient
            ug_code = next(iter(info["tabi_ug_codes"]), None)
            ug_title = next(iter(info["tabi_ug_titles"]), None)
            level = None
            canonical_title = next(iter(info["tabiturient_titles"]), None)

        g = groups[ug_code or "UNKNOWN"]
        g["ug_code"] = ug_code
        g["ug_title"] = ug_title

        g["directions"].append(
            {
                "direction_code": code,
                "title": canonical_title,
                "level": level,  # bach/spec если нашлось в ОКСО
                "sets": sorted(info["sets"]),
                "urls": sorted(info["urls"]),
                "tabiturient_titles": sorted(info["tabiturient_titles"]),
            }
        )

    # сортируем группы и направления
    merged = list(groups.values())
    merged.sort(key=lambda x: (x["ug_code"] or "ZZZ"))

    for g in merged:
        g["directions"].sort(key=lambda d: d["direction_code"])

    # сохраним
    with open(OUT_MERGED, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    report = {
        "tabiturient_unique_directions": len(by_dir),
        "matched_with_okso": matched,
        "unmatched_with_okso": unmatched,
        "match_rate": matched / max(1, (matched + unmatched)),
        "sets_counts": dict(
            sorted(
                {
                    "rus_math_ict": sum(1 for v in by_dir.values() if "rus_math_ict" in v["sets"]),
                    "rus_math_phys": sum(1 for v in by_dir.values() if "rus_math_phys" in v["sets"]),
                }.items()
            )
        ),
    }

    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("Saved:")
    print(" -", OUT_MERGED)
    print(" -", OUT_REPORT)
    print("Report:", report)


if __name__ == "__main__":
    main()
