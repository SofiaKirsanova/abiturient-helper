#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from typing import List, Dict, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]

IN_PATHS = {
    "postupi": REPO_ROOT / "data/processed/aggregators/postupi_moscow_vuz.names.json",
    "tabiturient": REPO_ROOT / "data/processed/aggregators/tabiturient_moscow_mo_vuz.names.json",
    "ucheba": REPO_ROOT / "data/processed/aggregators/ucheba_moscow_mo_vuz.names.json",
}

OUT_DIR = REPO_ROOT / "data/processed/aggregators_clean"

# --- helpers ---

def _load_json_list(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {path}, got {type(data)}")
    return [x for x in data if isinstance(x, str)]


def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _norm_spaces(s: str) -> str:
    s = s.replace("\u00a0", " ")  # NBSP
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _should_drop_by_substrings(s_lower: str, substrings: List[str]) -> bool:
    return any(sub in s_lower for sub in substrings)


def _has_vuz_keyword(s: str) -> bool:
    sl = s.lower()
    return any(k in sl for k in ["университет", "академ", "институт", "консерват", "высш", "политех"])

# --- cleaning rules ---

POSTUPI_DROP_EXACT = {
    "Вузы Москвы: список университетов и институтов",
}

TABITURIENT_DROP_SUBSTR = [
    # menu / ui
    "вход", "регистрация", "поиск вуза", "поиск специальности", "калькулятор",
    "рейтинги", "дни открытых дверей", "сравнить вузы", "статьи", "вопросы",
    "премиум", "политика конфиденциальности", "пользовательское соглашение",
    "карта сайта", "реклама", "обратная связь",
    # errors / loading
    "oops", "что-то пошло не так", "пропал интеренет", "загрузить еще", "идет загрузка",
    "пожалуйста, подождите",
    # qa / events
    "аноним", "вопрос :", "ответить", "день открытых дверей", "подробнее",
]

TABITURIENT_CUTOFF_MARKERS = [
    " Москва и Московская область ",
    " Москва ",
    " Московская область ",
    " Государственный ",
    " Головной ",
]

UCHEBA_DROP_SUBSTR = [
    # big header / ui
    "Вузы Москвы и области", "Найти", "Специальности", "Условия обучения",
    "Инструкция по применению", "Ок, я понял", "Новый поиск",
    "Найдено", "Показать карту", "Станции метро", "Выбрать субъект",
    # ads / projects
    "Хочешь сдать ЕГЭ", "онлайн-школе", "Cпецпроекты", "Спецпроекты",
]

UCHEBA_CUT_RE = re.compile(
    r"\s+(Прох\.|Прох|Бюдж\.|Бюдж|Стоимость|программ\b|мест\b)\s+",
    flags=re.IGNORECASE
)

# subunit must START with these (strict!)
SUBUNIT_PREFIX_RE = re.compile(
    r"^(факультет|школа|юридический институт|институт)\b",
    flags=re.IGNORECASE
)

# --- cleaners ---

def clean_postupi(names: List[str]) -> Tuple[List[str], Dict]:
    out = []
    dropped = 0
    for s in names:
        s = _norm_spaces(s)
        if not s:
            continue
        if s in POSTUPI_DROP_EXACT:
            dropped += 1
            continue
        out.append(s)
    out = _dedupe_keep_order(out)
    return out, {"dropped_exact": dropped, "kept": len(out)}


def clean_tabiturient(names: List[str]) -> Tuple[List[str], Dict]:
    out = []
    dropped = 0
    extracted_from_long = 0

    ABBR_OK = {
        "ФУ", "НИУ", "МЭИ", "РУДН", "МГТУ", "МАИ", "МФТИ", "МПГУ", "МГЮА", "РТУ",
        "МИРЭА", "РАНХИГС", "РОСБИОТЕХ", "РНИМУ", "МГМУ", "РГАУ-МСХА", "РХТУ",
        "РГУ", "РГГУ", "МГСУ", "МИФИ", "МИИТ", "ВШЭ", "СТАНКИН"
    }

    # если начинается с этого — почти всегда фрагмент (не имя)
    DROP_PREFIXES = (
        "при правительстве",  # при Правительстве РФ ...
    )

    for raw in names:
        s = _norm_spaces(raw)
        if not s:
            continue
        if s.lower().startswith("при правительстве рф "):
            # это почти всегда обрезок, ниже всё равно есть нормальная строка "Финансовый Университет..."
            dropped += 1
            continue


        sl = s.lower()
        if _should_drop_by_substrings(sl, TABITURIENT_DROP_SUBSTR):
            dropped += 1
            continue

        # cut big records at known markers
        cut_pos = None
        for m in TABITURIENT_CUTOFF_MARKERS:
            idx = s.find(m)
            if idx != -1:
                cut_pos = idx
                break

        s2 = _norm_spaces(s[:cut_pos]) if cut_pos is not None else s

        if len(s2) > 220:
            dropped += 1
            continue

        # quick drop by bad prefixes
        if s2.lower().startswith(DROP_PREFIXES):
            dropped += 1
            continue

        # Pass 1: strip obvious junk prefixes
        s2 = re.sub(r"^\s*(при\s+|им\.?\s+|имени\s+|-\s+|\(\s*МИИТ\s*\)\s+)", "", s2, flags=re.IGNORECASE)
        s2 = _norm_spaces(s2)

        # remove known abbreviation token only
        tokens = s2.split(" ")
        if len(tokens) >= 3:
            t0 = tokens[0].strip()
            t0_clean = re.sub(r"[«»\"'().]", "", t0).upper()
            if t0_clean in ABBR_OK:
                s2 = _norm_spaces(" ".join(tokens[1:]))
                extracted_from_long += 1

        # normalize MIRЭA duplicate pattern and leading "- "
        s2 = re.sub(r'^(МИРЭА\s+МИРЭА\s*[-—]\s*)', "МИРЭА — ", s2, flags=re.IGNORECASE)
        s2 = re.sub(r"^\s*-\s+", "", s2)
        s2 = _norm_spaces(s2)

        # Pass 2: strip again (реально встречается "им. Баумана ..." после других операций)
        s2 = re.sub(r"^\s*(им\.?\s+|имени\s+|-\s+)", "", s2, flags=re.IGNORECASE)
        s2 = _norm_spaces(s2)

        # Drop tail fragments like "Баумана ...", "Сеченова ...", "Пирогова ..."
        if s2.lower().startswith(("баумана ", "сеченова ", "пирогова ", "правительстве ")):
            dropped += 1
            continue
                # --- anchor-cut: убираем хвосты вида "Кутафина Московский ...", "Д. И. Менделеева Российский ...", etc.
        # если до якоря нет вуз-ключевых слов, то режем до якоря
        ANCHORS = [
            "ФГБОУ", "Федеральное", "Первый",
            "Московский", "Московская",
            "Российский", "Российская",
            "Национальный", "Национальная",
            "Финансовый", "Финансовая",
        ]
        for a in ANCHORS:
            m = re.search(rf"\b{re.escape(a)}\b", s2)
            if not m:
                continue
            prefix = s2[:m.start()].strip()
            # режем только если префикс не похож на нормальное начало названия вуза
            if prefix and (not _has_vuz_keyword(prefix)) and len(prefix) <= 40:
                s2 = _norm_spaces(s2[m.start():])
                break

        # Final: must contain vuz keyword, иначе выкидываем (кроме кратких аббревиатур)
        if not _has_vuz_keyword(s2):
            # разрешаем только короткие ALLCAPS-аббревиатуры (редко, но на всякий)
            if re.fullmatch(r"[A-ZА-ЯЁ0-9\-]{2,12}", s2):
                out.append(s2)
                continue
            dropped += 1
            continue
        
        # fix quotes / trailing garbage
        s2 = s2.replace("“", "\"").replace("”", "\"").replace("«", "«").replace("»", "»")
        s2 = re.sub(r'"+\s*$', '"', s2)  # много кавычек в конце -> одна
        s2 = re.sub(r'\s+"$', '"', s2)
        s2 = _norm_spaces(s2)

        # canonicalize Kosygin: приводим к одному имени (без ФГБОУ префикса)
        if "косыгина" in s2.lower():
            s2 = 'Российский государственный университет имени А.Н. Косыгина (Технологии.Дизайн.Искусство)'
                # canonicalize Polytech duplicates
        if s2.lower().startswith("московский политех"):
            s2 = "Московский политехнический университет"
                # canonicalize MGSU
        if "строительный университет" in s2.lower() and "московск" in s2.lower():
            # короткая версия -> каноническая
            s2 = "Национальный исследовательский Московский государственный строительный университет"



        out.append(s2)

    out = _dedupe_keep_order(out)
    return out, {"dropped": dropped, "extracted_from_long": extracted_from_long, "kept": len(out)}



def clean_ucheba(names: List[str]) -> Tuple[List[str], Dict, List[str]]:
    out = []
    subunits = []
    dropped = 0
    cut_count = 0

    for raw in names:
        s = _norm_spaces(raw)
        if not s:
            continue

        sl = s.lower()

        # hard drop explicit promo line that still slips through
        if sl.startswith("участвуй в конкурсе"):
            dropped += 1
            continue

        # Drop huge promo/article blocks BEFORE anything else (so they don't go to subunits either)
        if len(s) > 220:
            # sometimes a real name can be long, but not 220+ in this dataset
            dropped += 1
            continue

        # drop generic UI/header substrings unless it looks like a vuz name
        if _should_drop_by_substrings(sl, [x.lower() for x in UCHEBA_DROP_SUBSTR]):
            if not _has_vuz_keyword(s):
                dropped += 1
                continue

        # cut at metrics part
        m = UCHEBA_CUT_RE.search(s)
        if m:
            s2 = _norm_spaces(s[:m.start()])
            cut_count += 1
        else:
            s2 = s

        if not s2:
            continue

        # drop concatenations: too many "университет/институт/академ"
        if sum(1 for _ in re.finditer(r"(университет|институт|академ)", s2.lower())) >= 3:
            dropped += 1
            continue

        s2_low = s2.lower()

        # strict subunit detection: must START with факультет/школа/институт/...
        if SUBUNIT_PREFIX_RE.match(s2):
            # must also reference a parent university in wording
            if any(w in s2_low for w in ["университета", "университет", "академии", "академия"]):
                subunits.append(s2)
                continue

        # candidate must contain vuz keyword
        if not _has_vuz_keyword(s2):
            dropped += 1
            continue

        out.append(s2)

    out = _dedupe_keep_order(out)
    subunits = _dedupe_keep_order(subunits)
    return out, {"dropped": dropped, "cut_count": cut_count, "kept": len(out), "subunits_kept": len(subunits)}, subunits


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    postupi = _load_json_list(IN_PATHS["postupi"])
    tabiturient = _load_json_list(IN_PATHS["tabiturient"])
    ucheba = _load_json_list(IN_PATHS["ucheba"])

    postupi_clean, postupi_stats = clean_postupi(postupi)
    tab_clean, tab_stats = clean_tabiturient(tabiturient)
    ucheba_clean, ucheba_stats, ucheba_subunits = clean_ucheba(ucheba)

    _save_json(OUT_DIR / "postupi.names.clean.json", postupi_clean)
    _save_json(OUT_DIR / "tabiturient.names.clean.json", tab_clean)
    _save_json(OUT_DIR / "ucheba.names.clean.json", ucheba_clean)
    _save_json(OUT_DIR / "ucheba.subunits.json", ucheba_subunits)

    stats = {
        "inputs": {k: str(v) for k, v in IN_PATHS.items()},
        "postupi": postupi_stats,
        "tabiturient": tab_stats,
        "ucheba": ucheba_stats,
    }
    _save_json(OUT_DIR / "clean_stats.json", stats)

    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
