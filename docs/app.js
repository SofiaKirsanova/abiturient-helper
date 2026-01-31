async function loadData() {
  const res = await fetch("./data/merged_moscow_sets.json?v=810084d");
  if (!res.ok) throw new Error("Cannot load data: " + res.status);
  return await res.json();
}

function norm(s) { return (s || "").toLowerCase().trim(); }

function setLabel(key) {
  if (key === "rus_math_ict") return "ИКТ";
  if (key === "rus_math_phys") return "Ф";
  return key;
}

function hasSet(dir, setKey) {
  return (dir.sets || []).includes(setKey);
}

function matchesSet(dir, setFilter) {
  if (setFilter === "any") return true;
  if (setFilter === "both") return hasSet(dir, "rus_math_ict") && hasSet(dir, "rus_math_phys");
  return hasSet(dir, setFilter);
}

function matchesQuery(dir, q) {
  if (!q) return true;
  const nq = norm(q);
  return norm(dir.direction_code).includes(nq) || norm(dir.title).includes(nq);
}

function pickFirstUrl(dir) {
  return (dir.urls && dir.urls.length > 0) ? dir.urls[0] : null;
}

function render(data, setFilter, q) {
  const app = document.getElementById("app");
  app.innerHTML = "";

  let totalDirs = 0;
  let shownDirs = 0;
  let shownGroups = 0;

  for (const g of data) {
    const dirs = g.directions || [];
    totalDirs += dirs.length;

    const filtered = dirs.filter(d => matchesSet(d, setFilter) && matchesQuery(d, q));
    if (filtered.length === 0) continue;

    shownGroups += 1;
    shownDirs += filtered.length;

    const details = document.createElement("details");
    details.className = "group";

    // Авто-раскрытие, если поиск или фильтр узкий
    if (q || setFilter !== "any") details.open = true;

    const summary = document.createElement("summary");
    const ugCode = g.ug_code ?? "??.00.00";
    const ugTitle = g.ug_title ?? "";

    summary.innerHTML = `
      <span class="ug-code">${ugCode}</span>
      <span class="ug-title">${ugTitle}</span>
      <span class="count">${filtered.length}</span>
    `;
    details.appendChild(summary);

    const body = document.createElement("div");
    body.className = "group-body";

    for (const d of filtered) {
      const item = document.createElement("div");
      item.className = "dir";

      const pills = (d.sets || [])
        .filter(s => s === "rus_math_ict" || s === "rus_math_phys")
        .map(s => `<span class="pill">${setLabel(s)}</span>`)
        .join("");

      const url = pickFirstUrl(d);
      const linkHtml = url ? ` · <a href="${url}" target="_blank" rel="noopener noreferrer">tabiturient</a>` : "";

      item.innerHTML = `
        <div class="dir-top">
          <div class="dir-title"><b>${d.direction_code}</b> — ${d.title ?? "(нет названия)"}</div>
          <div class="pills">${pills}</div>
        </div>
        <div class="note">Русский + Математика${linkHtml}</div>
      `;

      body.appendChild(item);
    }

    details.appendChild(body);
    app.appendChild(details);
  }

  document.getElementById("stats").textContent =
    `групп: ${shownGroups} · направлений: ${shownDirs} из ${totalDirs}`;
}

/* --- Персональные фразы для Миши --- */
const SUPPORT_QUOTES = [
  "Миша, твоя задача — не угадать “идеальный” выбор, а собрать варианты и спокойно сузить. Это уже победа.",
  "Каждый день, когда ты делаешь маленький шаг (смотреть направления/вуз/проходные), ты становишься сильнее. Так и делается поступление.",
  "Выбирай не “навсегда”, а на ближайший разумный этап. Дальше всегда можно поворачивать.",
  "Если мозг устал — это нормально. 20 минут и пауза лучше, чем 2 часа через силу.",
  "У тебя уже есть база: предметы, интерес и команда поддержки. Остальное — техника.",
  "Не сравнивай свой путь с чужим. Твой план может быть умнее и спокойнее.",
  "Сомнения — признак того, что ты думаешь. Это не слабость, это качество.",
  "Мы делаем каталог, чтобы ты не тонул в хаосе. Дальше будет легче."
];

function setRandomSupportText() {
  const el = document.getElementById("supportText");
  const i = Math.floor(Math.random() * SUPPORT_QUOTES.length);
  el.textContent = SUPPORT_QUOTES[i];
}

async function main() {
  const data = await loadData();

  const setFilterEl = document.getElementById("setFilter");
  const qEl = document.getElementById("q");

  function rerender() {
    render(data, setFilterEl.value, qEl.value);
  }

  setFilterEl.addEventListener("change", rerender);
  qEl.addEventListener("input", rerender);

  document.getElementById("supportBtn").addEventListener("click", setRandomSupportText);
  setRandomSupportText();

  rerender();
}

main().catch(err => {
  const app = document.getElementById("app");
  app.innerHTML = `<div style="padding:14px; border:1px solid rgba(255,255,255,.15); border-radius:16px; background: rgba(255,255,255,.05);">
    Ошибка загрузки данных: ${String(err)}
  </div>`;
});
