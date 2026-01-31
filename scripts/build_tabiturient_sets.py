import re
import json
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

TABI_URLS = {
    "rus_math_ict": "https://tabiturient.ru/city/moscow/np/?110010000000=",
    "rus_math_phys": "https://tabiturient.ru/city/moscow/np/?110000001000=",
}


CODE_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{2})\b")
DIR_CODE_RE = re.compile(r"^\d{2}\.(03|05)\.\d{2}$")

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

@dataclass
class DirectionRow:
    set_key: str
    ug_code: Optional[str]
    ug_title: Optional[str]
    direction_code: str
    direction_title: str
    url: Optional[str]

def fetch(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text

def parse_page(set_key: str, url: str) -> List[DirectionRow]:
    """
    Парсит страницу вида /city/moscow/np/?<mask>=
    На этих страницах часто:
      - "Укрупненная группа XX.00.00" лежит в <span>
      - Код направления лежит в <span>Бакалавриат | XX.03.YY</span> или <span>Специалитет | XX.05.YY</span>
      - Название направления обычно в ближайшем <b>...</b> перед этим span
    """
    html = fetch(url)

    raw_path = f"data/raw/tabiturient_{set_key}.html"
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "lxml")

    rows: List[DirectionRow] = []

    ug_span_re = re.compile(r"Укрупненная группа\s+(\d{2}\.\d{2}\.\d{2})")
    code_span_re = re.compile(r"(Бакалавриат|Специалитет)\s*\|\s*(\d{2}\.(?:03|05)\.\d{2})")

    ug_code = None
    ug_title = None

    # Проходим по всем span, где может быть "Укрупненная группа" или "Бакалавриат|код"
    for sp in soup.find_all("span"):
        st = clean(sp.get_text(" ", strip=True))

        # 1) Укрупненная группа
        m_ug = ug_span_re.search(st)
        if m_ug:
            ug_code = m_ug.group(1)

            # название группы — ближайший жирный/крупный текст выше
            ug_title = None
            prev_b = sp.find_previous("b")
            if prev_b:
                ug_title = clean(prev_b.get_text(" ", strip=True))
            continue

        # 2) Бакалавриат/Специалитет + код направления
        m_code = code_span_re.search(st)
        if not m_code:
            continue

        direction_code = m_code.group(2)

        # название направления — ближайший <b> перед этим span
        title = None
        b = sp.find_previous("b")
        if b:
            title = clean(b.get_text(" ", strip=True))

        if not title:
            continue

        # (опционально) url: иногда есть ссылка рядом; попробуем найти ближайший <a> выше
        full_url = None
        a = sp.find_previous("a")
        if a:
            href = a.get("href") or ""
            if href.startswith("/"):
                full_url = urljoin("https://tabiturient.ru", href)
            elif href.startswith("http"):
                full_url = href

        rows.append(
            DirectionRow(
                set_key=set_key,
                ug_code=ug_code,
                ug_title=ug_title,
                direction_code=direction_code,
                direction_title=title,
                url=full_url,
            )
        )

    # дедуп по (set_key, direction_code)
    dedup = {}
    for r in rows:
        dedup[(r.set_key, r.direction_code)] = r

    return list(dedup.values())


def main():
    all_rows: List[DirectionRow] = []

    for set_key, url in TABI_URLS.items():
        all_rows.extend(parse_page(set_key, url))

    out = [asdict(r) for r in all_rows]
    with open("data/processed/tabiturient_sets.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("Saved: data/processed/tabiturient_sets.json")
    print("Rows:", len(out))

    # быстрый sanity print
    by_set = {}
    for r in out:
        by_set.setdefault(r["set_key"], 0)
        by_set[r["set_key"]] += 1
    print("By set:", by_set)

if __name__ == "__main__":
    main()
