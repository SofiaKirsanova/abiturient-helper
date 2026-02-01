#!/usr/bin/env python3
from __future__ import annotations
import re, json, argparse, os
from bs4 import BeautifulSoup

def norm(s: str) -> str:
    s = s.lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("«", '"').replace("»", '"')
    return s

def extract_text_candidates(html: str) -> list[str]:
    # fallback: берём все “похожие на название вуза” куски текста
    soup = BeautifulSoup(html, "html.parser")
    texts = []
    for tag in soup.find_all(["a","h1","h2","h3","div","span","p","li"]):
        t = tag.get_text(" ", strip=True)
        if not t:
            continue
        # грубый фильтр: оставим строки, где часто встречается вузовая лексика
        tl = t.lower()
        if any(x in tl for x in ["университет","институт","академ", "высшая школа", "федеральное государственное", "государственный университет"]):
            texts.append(t)
    return texts

def parse_file(path: str) -> list[str]:
    with open(path, "rb") as f:
        raw = f.read()

    soup = BeautifulSoup(raw, "lxml")

    # Слова, которые почти гарантированно означают "карточка/интерфейс/статистика/новость"
    bad_substrings = [
        "ср. балл", "средний балл", "бюджетных мест", "платных мест",
        "информация о стоимости", "стоимость обучения", "/ год",
        "егэ", "олимпиад", "калькулятор", "фильтр", "сортировка", "применить",
        "поделиться", "обратная связь", "дни открытых дверей", "календарь абитуриента",
        "подписка", "тесты", "консультац",
    ]

    # Маркеры "новость/событие", их надо выкидывать
    news_markers = [
        "начал", "стартова", "провед", "приглаша", "финал", "договор",
        "предлож", "возможност", "перевод", "сотрудничеств", "обмен", "совместн",
    ]

    # Слишком общие "названия"
    generic_drop = {"институт", "университет", "академия", "колледж", "вуз", "филиал", "школа"}

    def looks_bad(t: str) -> bool:
        tl = t.lower()
        if len(t) > 140:
            return True
        if any(x in tl for x in bad_substrings):
            return True
        if any(x in tl for x in news_markers) and len(t.split()) >= 6:
            return True
        # карточки почти всегда содержат много цифр (цены, годы, баллы)
        if len(re.findall(r"\d", t)) >= 3:
            return True
        return False

    def cleanup_title(t: str) -> str:
        t = t.replace("\u00a0", " ")
        t = re.sub(r"\s+", " ", t).strip()

        # отрезать направления после ';'
        t = re.sub(r";.*$", "", t).strip()
        # отрезать хвост "и ещё N направлений"
        t = re.sub(r"\s+и ещё\s+\d+\s+направлен.*$", "", t, flags=re.I).strip()

        # если строка начинается с "Москва Государственный/Негосударственный ..."
        if t.startswith("Москва "):
            t = t[len("Москва "):].strip()
            t = re.sub(r"^(Государственный|Негосударственный)\s+", "", t, flags=re.I).strip()

        return t

    texts = []
    # КЛЮЧЕВОЕ: берем только теги, которые вероятнее всего содержат чистое название (a/h*)
    for tag in soup.find_all(["a", "h1", "h2", "h3"]):
        t = tag.get_text(" ", strip=True)
        if not t:
            continue

        t = cleanup_title(t)
        if not t:
            continue

        tl = t.lower()
        # вузовая лексика
        if not any(x in tl for x in ["университет", "институт", "академ", "высшая школа"]):
            continue

        if looks_bad(t):
            continue

        if tl in generic_drop or len(t) < 8:
            continue

        texts.append(t)

    # дедуп
    seen = set()
    out = []
    for x in texts:
        k = norm(x)
        if k in seen:
            continue
        seen.add(k)
        out.append(x)

    return out



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data/processed/aggregators")
    ap.add_argument("inputs", nargs="+")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    for p in args.inputs:
        name = os.path.splitext(os.path.basename(p))[0]
        items = parse_file(p)
        out_path = os.path.join(args.outdir, f"{name}.names.json")
        with open(out_path, "w", encoding="utf-8") as w:
            json.dump(items, w, ensure_ascii=False, indent=2)
        print(name, "->", len(items), "names")

if __name__ == "__main__":
    main()
