import json

OKSO = "data/processed/okso_1061.json"
TABI = "data/processed/tabiturient_sets.json"

okso = json.load(open(OKSO, "r", encoding="utf-8"))
tabi = json.load(open(TABI, "r", encoding="utf-8"))

okso_codes = {r["code"] for r in okso}
tabi_codes = {r["direction_code"] for r in tabi}

unmatched = sorted(tabi_codes - okso_codes)

print("Unmatched codes:", unmatched)
print("Count:", len(unmatched))

# покажем, как они называются у tabiturient
by_code = {}
for r in tabi:
    by_code.setdefault(r["direction_code"], set()).add(r["direction_title"])

for c in unmatched:
    print("\n", c)
    for t in sorted(by_code.get(c, [])):
        print("  -", t)
