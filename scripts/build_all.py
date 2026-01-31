"""
build_all.py

Зачем:
- Один входной скрипт для обновления всех данных и копирования результата в site/data.
- Удобно для локального запуска и для GitHub Actions cron.

Что делает:
1) Собирает okso_1061.json (официальный перечень + patch)
2) Собирает tabiturient_sets.json (2 набора ЕГЭ: Р+М+ИКТ и Р+М+Физика, Москва/МО)
3) Делает merge -> merged_moscow_sets.json
4) Копирует merged JSON в site/data для сайта
"""

import os
import shutil
import subprocess
from pathlib import Path

def run(cmd):
    print(">", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    run(["python", "scripts/build_okso_1061_from_kontur.py"])
    run(["python", "scripts/build_tabiturient_sets.py"])
    run(["python", "scripts/merge_okso_with_tabiturient.py"])

    Path("site/data").mkdir(parents=True, exist_ok=True)
    shutil.copyfile("data/processed/merged_moscow_sets.json", "site/data/merged_moscow_sets.json")

    print("OK: site/data/merged_moscow_sets.json updated")

if __name__ == "__main__":
    main()
