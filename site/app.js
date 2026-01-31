async function loadData() {
    const res = await fetch("./data/merged_moscow_sets.json");
    if (!res.ok) throw new Error("Cannot load data: " + res.status);
    return await res.json();
  }
  
  function norm(s) { return (s || "").toLowerCase().trim(); }
  
  function setLabel(key) {
    if (key === "rus_math_ict") return "Р+М+ИКТ";
    if (key === "rus_math_phys") return "Р+М+Физ";
    return key;
  }
  
  function matchesSet(dir, setFilter) {
    if (setFilter === "any") return true;
    return (dir.sets || []).includes(setFilter);
  }
  
  function matchesQuery(dir, q) {
    if (!q) return true;
    const nq = norm(q);
    return norm(dir.direction_code).includes(nq) || norm(dir.title).includes(nq);
  }
  
  function render(data, setFilter, q) {
    const app = document.getElementById("app");
    app.innerHTML = "";
  
    let total = 0, shown = 0;
  
    for (const g of data) {
      const dirs = g.directions || [];
      total += dirs.length;
  
      const filtered = dirs.filter(d => matchesSet(d, setFilter) && matchesQuery(d, q));
      if (filtered.length === 0) continue;
      shown += filtered.length;
  
      const h2 = document.createElement("h2");
      h2.textContent = `${g.ug_code ?? "??.00.00"} — ${g.ug_title ?? ""}`;
      app.appendChild(h2);
  
      for (const d of filtered) {
        const item = document.createElement("div");
        item.className = "dir";
  
        const title = document.createElement("div");
        title.innerHTML = `<b>${d.direction_code}</b> — ${d.title ?? "(нет названия)"}`;
        item.appendChild(title);
  
        const meta = document.createElement("div");
        meta.className = "muted";
        const pills = (d.sets || []).map(s => `<span class="pill">${setLabel(s)}</span>`).join("");
        meta.innerHTML = `${pills}`;
        item.appendChild(meta);
  
        if ((d.urls || []).length > 0) {
          const links = document.createElement("div");
          links.className = "muted";
          links.innerHTML = `tabiturient: <a href="${d.urls[0]}" target="_blank" rel="noopener noreferrer">открыть</a>`;
          item.appendChild(links);
        }
  
        app.appendChild(item);
      }
    }
  
    document.getElementById("stats").textContent = `показано: ${shown} из ${total}`;
  }
  
  async function main() {
    const data = await loadData();
    const setFilterEl = document.getElementById("setFilter");
    const qEl = document.getElementById("q");
  
    const rerender = () => render(data, setFilterEl.value, qEl.value);
  
    setFilterEl.addEventListener("change", rerender);
    qEl.addEventListener("input", rerender);
  
    rerender();
  }
  
  main().catch(err => {
    document.getElementById("app").innerHTML =
      `<div class="box">Ошибка: ${String(err)}</div>`;
  });
  