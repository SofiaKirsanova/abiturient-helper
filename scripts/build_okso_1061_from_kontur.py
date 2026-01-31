import re
import json
from dataclasses import dataclass, asdict
from typing import List, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

KONTUR_URL = "https://normativ.kontur.ru/document?documentId=391201&moduleId=1"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

CODE_RE = re.compile(r"^\d{2}\.\d{2}\.\d{2}$")
UG_RE = re.compile(r"^\d{2}\.00\.00$")
BACH_RE = re.compile(r"^\d{2}\.03\.\d{2}$")  # бакалавриат
SPEC_RE = re.compile(r"^\d{2}\.05\.\d{2}$")  # специалитет


@dataclass
class OksoRow:
    level: str                # "bach" | "spec"
    ug_code: Optional[str]    # 09.00.00
    ug_title: Optional[str]   # Информатика и вычислительная техника
    code: str                 # 09.03.04
    title: str                # Программная инженерия


def fetch_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def extract_tables(html: str) -> List[pd.DataFrame]:
    """
    Извлекаем все <table> со страницы (пандой).
    """
    try:
        return pd.read_html(StringIO(html))
    except ValueError:
        return []


def rows_from_df(df: pd.DataFrame, level: str) -> List[OksoRow]:
    """
    Вытаскиваем строки ОКСО из одной таблицы:
    - строки укрупнённых групп: XX.00.00
    - строки направлений/спец.: XX.03.YY / XX.05.YY
    """
    out: List[OksoRow] = []

    df2 = df.copy()
    df2.columns = [f"c{i}" for i in range(len(df2.columns))]
    df2 = df2.fillna("")

    ug_code = None
    ug_title = None

    for _, row in df2.iterrows():
        vals = [normalize_text(str(row[c])) for c in df2.columns]
        vals = [v for v in vals if v]

        if not vals:
            continue

        code = None
        title = None

        # Чаще всего: [код, название]
        if len(vals) >= 2 and CODE_RE.match(vals[0]):
            code = vals[0]
            title = vals[1]
        else:
            # иначе ищем код в любой ячейке
            for i, v in enumerate(vals):
                if CODE_RE.match(v):
                    code = v
                    title = vals[i + 1] if i + 1 < len(vals) else " ".join([x for j, x in enumerate(vals) if j != i])
                    break

        if not code or not title:
            continue

        title = normalize_text(title)

        # Укрупнённая группа: XX.00.00
        if UG_RE.match(code):
            ug_code = code[:2] + ".00.00"  # на всякий
            ug_title = title
            continue

        # Фильтр по уровню
        if level == "bach" and not BACH_RE.match(code):
            continue
        if level == "spec" and not SPEC_RE.match(code):
            continue

        out.append(OksoRow(level=level, ug_code=ug_code, ug_title=ug_title, code=code, title=title))

    return out


def main():
    html = fetch_html(KONTUR_URL)

    # raw snapshot
    with open("data/raw/kontur_1061.html", "w", encoding="utf-8") as f:
        f.write(html)

    dfs = extract_tables(html)
    if not dfs:
        raise RuntimeError("Не нашёл ни одной <table> на странице (pandas.read_html вернул пусто).")

    rows: List[OksoRow] = []
    for t in dfs:
        rows.extend(rows_from_df(t, "bach"))
        rows.extend(rows_from_df(t, "spec"))

    # дедуп по (level, code)
    seen = set()
    deduped = []
    for r in rows:
        k = (r.level, r.code)
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    
    # --- OPTIONAL PATCH: дополняем отсутствующие коды (если источник не отдал часть таблиц) ---
    patch_path = "data/manual/okso_patch.json"
    try:
        with open(patch_path, "r", encoding="utf-8") as f:
            patch = json.load(f)
    except FileNotFoundError:
        patch = []

    ug_title_by_code = {}
    for r in deduped:
        if r.ug_code and r.ug_title:
            ug_title_by_code[r.ug_code] = r.ug_title

    existing = {(r.level, r.code) for r in deduped}

    for p in patch:
        level = p["level"]
        code = p["code"]
        title = p["title"]

        if (level, code) in existing:
            continue

        ug_code = code[:2] + ".00.00"
        ug_title = ug_title_by_code.get(ug_code)

        deduped.append(
            OksoRow(
                level=level,
                ug_code=ug_code,
                ug_title=ug_title,
                code=code,
                title=title,
            )
        )
        existing.add((level, code))


    okso = [asdict(r) for r in deduped]
    with open("data/processed/okso_1061.json", "w", encoding="utf-8") as f:
        json.dump(okso, f, ensure_ascii=False, indent=2)

    df_out = pd.DataFrame(okso)
    df_out.to_csv("data/processed/okso_1061.csv", index=False, encoding="utf-8-sig")

    print("Saved:")
    print(" - data/processed/okso_1061.json")
    print(" - data/processed/okso_1061.csv")
    print(f"Rows: {len(df_out)}")
    print(df_out.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
