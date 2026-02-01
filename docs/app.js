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

function render(data, setFilter, q, sortBy) {
  const app = document.getElementById("app");
  app.innerHTML = "";

  let totalDirs = 0;
  let shownDirs = 0;
  let shownGroups = 0;

  // Собираем группы с учётом фильтров
  const groups = [];
  for (const g of data) {
    const dirs = g.directions || [];
    totalDirs += dirs.length;

    const filtered = dirs
      .filter(d => matchesSet(d, setFilter) && matchesQuery(d, q))
      .slice()
      .sort((a, b) => (a.direction_code || "").localeCompare(b.direction_code || "", "ru"));

    if (filtered.length === 0) continue;

    groups.push({ g, filtered });
  }

  // Сортировка групп
  if (sortBy === "alpha") {
    groups.sort((x, y) => (x.g.ug_title || "").localeCompare(y.g.ug_title || "", "ru"));
  } else if (sortBy === "count_desc") {
    groups.sort((x, y) => y.filtered.length - x.filtered.length);
  } else {
    groups.sort((x, y) => (x.g.ug_code || "").localeCompare(y.g.ug_code || "", "ru"));
  }

  for (const { g, filtered } of groups) {
    shownGroups += 1;
    shownDirs += filtered.length;

    const details = document.createElement("details");
    details.className = "group";

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
        .map(s => {
          const cls = (s === "rus_math_ict") ? "pill pill-ict" : "pill pill-phys";
          return `<span class="${cls}">${setLabel(s)}</span>`;
        })
        .join("");


        item.innerHTML = `
        <div class="dir-top">
          <div class="dir-title"><b>${d.direction_code}</b> — ${d.title ?? "(нет названия)"}</div>
          <div class="pills">${pills}</div>
        </div>
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
  "Миша, твоя задача — не угадать “идеальный” выбор, не сделать всё сразу, а монотонно, маленкьими шагами идти к цели",
  "Каждый день, когда ты делаешь маленький шаг, ты становишься сильнее. Так и создаётся успех",
  "Выбирай не “навсегда”, а на ближайший разумный этап. Дальше всегда можно поворачивать",
  "20 минут и пауза лучше, чем 2 часа через силу",
  "У тебя уже есть база: желание успеха, любопытство и команда поддержки. Остальное — техника",
  "Не сравнивай свой путь с чужим",
  "Я сделала этот каталог, чтобы ты не тонул в хаосе. Дальше будет легче, обещаю!",
  "Быбчек, как известно, быбчеку рознь"
];

function setRandomSupportText() {
  const el = document.getElementById("supportText");
  const i = Math.floor(Math.random() * SUPPORT_QUOTES.length);
  el.textContent = SUPPORT_QUOTES[i];
}

async function main() {
  const data = await loadData();
    // Статистика для пояснения "откуда данные"
    const allDirs = [];
    for (const g of data) for (const d of (g.directions || [])) allDirs.push(d);
  
    const uniqCodes = new Set(allDirs.map(d => d.direction_code));
    const nTotal = uniqCodes.size;
  
    const nICT = new Set(allDirs.filter(d => (d.sets || []).includes("rus_math_ict")).map(d => d.direction_code)).size;
    const nPhys = new Set(allDirs.filter(d => (d.sets || []).includes("rus_math_phys")).map(d => d.direction_code)).size;
  
    const infoEl = document.getElementById("datasetInfo");
    if (infoEl) {
      infoEl.textContent =
        `Дотошному папе: сейчас в базе: ${nTotal} направлений (Москва/МО). ` +
        `Собрано автоматически с по наборам ЕГЭ Р+М+ИКТ (${nICT}) и Р+М+Ф (${nPhys}); ` +
        `коды/названия сверены с официальным перечнем ОКСО (приказ №1061).`;
    }
  
  const setFilterEl = document.getElementById("setFilter");
  const qEl = document.getElementById("q");
  const sortByEl = document.getElementById("sortBy");


  function rerender() {
    render(data, setFilterEl.value, qEl.value, sortByEl ? sortByEl.value : "ug_code");
  }  

  setFilterEl.addEventListener("change", rerender);
  qEl.addEventListener("input", rerender);
  if (sortByEl) sortByEl.addEventListener("change", rerender);

  const btn = document.getElementById("supportBtn");
  if (btn) btn.addEventListener("click", setRandomSupportText);
  setRandomSupportText();


  rerender();
}

main().catch(err => {
  const app = document.getElementById("app");
  app.innerHTML = `<div style="padding:14px; border:1px solid rgba(255,255,255,.15); border-radius:16px; background: rgba(255,255,255,.05);">
    Ошибка загрузки данных: ${String(err)}
  </div>`;
});
