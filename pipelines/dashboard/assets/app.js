"use strict";
// pipelines dashboard — a small single-page app. No framework, no build step.
//
//   • a left sidebar + a shared poll of /api/runs drive navigation and the header
//   • Runs (#/)  a stat-card summary + a filterable, paginated table of runs
//   • All runs (#/all)  every run expanded inline (polled /api/overview)
//   • Run detail (#/run/<port>)  SSE snapshot + live records, grouped into pipeline steps
//   • Log (#/run/<port>/log/<slug>)  an SSE tail with follow-on-scroll
//
// A run's jobs are organized the way `pipelines plan` shows them: grouped by pipeline step
// (artifact type, ordered by depth), already-committed artifacts shown as "cached". The StepsView
// component renders that and is shared by the run-detail page and the all-runs tab.

// --------------------------------------------------------------------------- //
// DOM + formatting helpers
// --------------------------------------------------------------------------- //
function h(tag, attrs, ...kids) {
  const node = document.createElement(tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") node.className = v;
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else if (v === true) node.setAttribute(k, "");
    else node.setAttribute(k, v);
  }
  for (const kid of kids.flat()) {
    if (kid == null || kid === false) continue;
    node.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  }
  return node;
}

function svgEl(viewBox, inner, size) {
  const ns = "http://www.w3.org/2000/svg";
  const s = document.createElementNS(ns, "svg");
  s.setAttribute("viewBox", viewBox); s.setAttribute("width", size); s.setAttribute("height", size);
  s.setAttribute("fill", "none"); s.setAttribute("stroke", "currentColor"); s.setAttribute("stroke-width", "1.8");
  s.setAttribute("stroke-linecap", "round"); s.setAttribute("stroke-linejoin", "round");
  s.innerHTML = inner;
  return s;
}
const iconSearch = () => svgEl("0 0 24 24", '<circle cx="11" cy="11" r="7"></circle><path d="M21 21l-3.6-3.6"></path>', 15);

const STATES = ["running", "yielding", "queued", "held", "blocked", "completed", "cached", "failed", "cancelled"];
const TERMINAL = new Set(["completed", "cached", "failed", "cancelled", "blocked"]);
const isActive = (s) => s === "running" || s === "yielding";

// Default order for a step's expanded instances: active on top, cached at the bottom.
const INST_RANK = { running: 0, yielding: 1, queued: 2, held: 3, blocked: 4, failed: 5, cancelled: 6, completed: 7, cached: 8 };

const stateColor = (s) => `var(--${STATES.includes(s) ? s : "queued"})`;
const dot = (s) => h("span", { class: "dot", style: `background:${stateColor(s)}` });
// Internal state strings come from the scheduler (e.g. "cancelled"); this is the user-facing label.
const stateLabel = (s) => s === "cancelled" ? "canceled" : s;

function slugify(v) {              // mirrors pipelines.identity.slug
  return String(v).replace(/[^A-Za-z0-9._-]+/g, "_").replace(/^_+|_+$/g, "").replace(/_+/g, "_");
}

function fmtDur(s) {
  if (s == null) return "—";
  s = Math.max(0, Math.floor(s));
  const hr = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  if (hr) return `${hr}h ${m}m`;
  if (m) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function fmtAgo(ts) {
  if (!ts) return "—";
  const d = serverNow() - ts;
  if (d < 45) return "just now";
  if (d < 5400) return `${Math.round(d / 60)}m ago`;
  if (d < 129600) return `${Math.round(d / 3600)}h ago`;
  return `${Math.round(d / 86400)}d ago`;
}

const liveElapsed = (j) => j.started_at == null ? null
  : (j.ended_at != null ? j.ended_at : serverNow()) - j.started_at;
const elapsedText = (j) => { const e = liveElapsed(j); return e == null ? "" : fmtDur(e); };
function gpuText(j) {
  if (j.gpus && j.gpus.length) return "gpu " + j.gpus.join(",");
  const r = j.req || {};
  return r.gpus ? `${r.gpus} gpu` : "";
}

async function api(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return r.json();
}

// --------------------------------------------------------------------------- //
// Shared run poll — feeds the header always, and the dashboard while mounted.
// Also tracks the server/client clock offset so elapsed times survive skew.
// --------------------------------------------------------------------------- //
let clockOffset = 0;
const serverNow = () => Date.now() / 1000 + clockOffset;

const Runs = {
  data: { runs: [], now: 0 },
  ok: null,
  subs: new Set(),
  timer: 0,
  start() {
    if (this.timer) return;
    this.tick();
    this.timer = setInterval(() => this.tick(), 1500);
  },
  async tick() {
    try {
      this.data = await api("/api/runs");
      clockOffset = (this.data.now || Date.now() / 1000) - Date.now() / 1000;
      this.ok = true;
    } catch {
      this.ok = false;
    }
    for (const fn of this.subs) fn(this.data, this.ok);
  },
  subscribe(fn) {
    this.subs.add(fn);
    fn(this.data, this.ok);
    return () => this.subs.delete(fn);
  },
};

// --------------------------------------------------------------------------- //
// Shared widgets
// --------------------------------------------------------------------------- //
function pill(status) {
  const cls = ["live", "completed", "failed"].includes(status) ? status : "interrupted";
  const tone = { live: "running", completed: "completed", failed: "failed", interrupted: "cancelled" }[status] || "queued";
  return h("span", { class: `pill ${cls}` }, dot(tone), status);
}

function statePill(state) {
  return h("span", { class: "pill" }, dot(state), stateLabel(state));
}

function segBar(counts, total, extra) {
  const bar = h("div", { class: "bar" + (extra ? " " + extra : "") });
  if (!total) return bar;
  for (const s of STATES) {
    const n = counts[s] || 0;
    if (n) bar.append(h("span", { style: `width:${(100 * n) / total}%;background:${stateColor(s)}`, title: `${s}: ${n}` }));
  }
  return bar;
}

// Full tallies (dot + count + word) — for roomy contexts.
function tallies(counts) {
  const wrap = h("div", { class: "tallies" });
  for (const s of STATES) {
    const n = counts[s] || 0;
    if (n) wrap.append(h("span", { class: "tally" }, dot(s), h("b", {}, n), stateLabel(s)));
  }
  return wrap;
}

// Compact tallies (dot + status name + count) — for step / run headers.
function miniTallies(counts) {
  const wrap = h("span", { class: "mini-tallies" });
  for (const s of STATES) {
    const n = counts[s] || 0;
    if (n) wrap.append(h("span", { class: "mt" + (isActive(s) ? " run" : "") },
      dot(s), h("span", { class: "mt-name" }, stateLabel(s)), h("b", {}, n)));
  }
  return wrap;
}

function statCard(label, num, tone) {
  return h("div", { class: "stat" },
    h("div", { class: "label" }, label),
    h("div", { class: "num" + (tone ? " " + tone : "") }, num));
}

function setCrumbs(items) {
  const c = document.getElementById("crumbs");
  c.replaceChildren();
  items.forEach((it, i) => {
    if (i) c.append(h("span", { class: "sep" }, "›"));
    c.append(it.href ? h("a", { href: it.href }, it.label) : h("span", { class: "here" }, it.label));
  });
}

function setNav(section) {
  for (const a of document.querySelectorAll("[data-nav]")) a.classList.toggle("on", a.dataset.nav === section);
}

function emptyState() {
  return h("div", { class: "empty" },
    h("div", { class: "big" }, "No runs yet"),
    h("div", {}, "Start one with ", h("code", {}, "pipelines runparallel"), " and it will show up here."));
}

// --------------------------------------------------------------------------- //
// StepsView — the pipeline structure: steps (artifact types) → instances.
// Shared by the run-detail page (fed via SSE) and the "all runs" tab (polled).
// Owns its own expand state, keyed so live updates never flicker.
// --------------------------------------------------------------------------- //
function StepsView(port, opts = {}) {
  const withHeader = opts.header !== false;       // run detail shows the sortable header; all-runs doesn't
  const el = h("div", { class: "steps-table" + (withHeader ? "" : " no-head") });
  const headerEl = withHeader ? h("div", { class: "steps-head steps-grid" }) : null;
  const emptyRow = h("div", { class: "empty small" }, "No artifacts.");
  const expanded = new Map();   // type -> bool (explicit user choice)
  const toggled = new Set();    // types the user has decided about (overrides the auto rule)
  const stepEls = new Map();    // type -> { block, head, list, instsHead, rows, step }
  const instSort = new Map();   // type -> { key, dir } for the per-step instance sub-table
  let steps = [];
  let sortKey = null, sortDir = -1;   // null = pipeline (server / depth) order

  const doneOf = (s) => s.instances.reduce((n, i) => n + (i.state === "completed" || i.state === "cached" ? 1 : 0), 0);
  const activeOf = (s) => s.instances.reduce((n, i) => n + (isActive(i.state) ? 1 : 0), 0);
  // A step is open if the user said so, else automatically while it has active work.
  const isOpen = (step) => toggled.has(step.type) ? !!expanded.get(step.type) : activeOf(step) > 0;

  function toggle(step) {
    const open = isOpen(step);
    toggled.add(step.type);
    expanded.set(step.type, !open);
    render();
  }

  function setData(next) { steps = next || []; render(); }

  // Each sortable column cycles: pipeline order → primary dir → opposite dir → pipeline order.
  const defaultDir = (key) => key === "name" ? 1 : -1;
  function clickSort(key) {
    if (sortKey !== key) { sortKey = key; sortDir = defaultDir(key); }
    else if (sortDir === defaultDir(key)) { sortDir = -sortDir; }
    else { sortKey = null; }
    render();
  }

  function sorted() {
    if (!sortKey) return steps;                   // pipeline order, as the server returned it
    const value = {
      name: (s) => s.type.toLowerCase(),
      count: (s) => s.total,
      running: (s) => activeOf(s),
      progress: (s) => s.total ? doneOf(s) / s.total : 0,
    }[sortKey];
    return steps.slice().sort((a, b) => {
      const va = value(a), vb = value(b);
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return a.type.localeCompare(b.type);
    });
  }

  // --- instance sub-table (Index / Name / Status / Elapsed), sortable per step --- //
  const instDefaultDir = (key) => key === "elapsed" ? -1 : 1;
  function instSortFor(type) {
    let s = instSort.get(type);
    if (!s) { s = { key: "status", dir: 1 }; instSort.set(type, s); }   // default: by status, running on top
    return s;
  }
  function clickInstSort(type, key) {
    const s = instSortFor(type);
    if (s.key !== key) { s.key = key; s.dir = instDefaultDir(key); }
    else { s.dir = -s.dir; }
    render();
  }
  function sortedInstances(step) {
    const s = instSortFor(step.type);
    const order = new Map(step.instances.map((it, n) => [it.relpath, n]));   // original (declaration) index
    const value = {
      index: (it) => order.get(it.relpath),
      name: (it) => (it.name || it.relpath).toLowerCase(),
      status: (it) => INST_RANK[it.state] ?? 99,
      elapsed: (it) => liveElapsed(it),
    }[s.key];
    return step.instances.slice().sort((a, b) => {
      const va = value(a), vb = value(b);
      if (s.key === "elapsed") {                       // queued / cached have none → always last
        if (va == null && vb != null) return 1;
        if (vb == null && va != null) return -1;
      }
      if (va < vb) return -s.dir;
      if (va > vb) return s.dir;
      return order.get(a.relpath) - order.get(b.relpath);   // stable tiebreak
    });
  }
  function renderInstsHead(se, step) {
    const s = instSortFor(step.type);
    const col = (label, key) => {
      const c = h("span", { class: "col" + (s.key === key ? " active" : "") },
        label, h("span", { class: "sort-ic" }, s.key === key ? (s.dir > 0 ? "↑" : "↓") : "↕"));
      c.addEventListener("click", (e) => { e.stopPropagation(); clickInstSort(step.type, key); });
      return c;
    };
    se.instsHead.replaceChildren(col("Index", "index"), col("Name", "name"), col("Status", "status"), col("Elapsed", "elapsed"));
  }

  function renderHeader() {
    if (!headerEl) return;
    const col = (label, key) => {
      const c = h("span", { class: "col sortable" + (sortKey === key ? " active" : "") },
        label, h("span", { class: "sort-ic" }, sortKey === key ? (sortDir > 0 ? "↑" : "↓") : "↕"));
      c.addEventListener("click", () => clickSort(key));
      return c;
    };
    headerEl.replaceChildren(
      h("span", {}),                              // caret column spacer
      col("Step", "name"), col("Artifacts", "count"), col("Status", "running"), col("Progress", "progress"));
  }

  function render() {
    if (headerEl) { renderHeader(); if (el.firstChild !== headerEl) el.prepend(headerEl); }
    if (!steps.length) {
      for (const [, se] of stepEls) se.block.remove();
      stepEls.clear();
      if (!emptyRow.isConnected) el.append(emptyRow);
      return;
    }
    if (emptyRow.isConnected) emptyRow.remove();
    const seen = new Set();
    for (const step of sorted()) {
      let se = stepEls.get(step.type);
      if (!se) { se = makeStep(); stepEls.set(step.type, se); }
      patchStep(se, step);
      el.append(se.block);                        // (re)order; moves existing nodes, no flicker
      seen.add(step.type);
    }
    for (const [t, se] of stepEls) if (!seen.has(t)) { se.block.remove(); stepEls.delete(t); }
  }

  function makeStep() {
    const se = { step: null };
    se.list = h("div", { class: "insts" });
    se.instsHead = h("div", { class: "insts-head insts-grid" });
    se.rows = new Map();
    se.head = h("div", { class: "step-head steps-grid", onclick: () => se.step && toggle(se.step) });
    se.block = h("div", { class: "step" }, se.head, se.list);
    return se;
  }

  function patchStep(se, step) {
    se.step = step;
    const counts = countsOf(step);
    const open = isOpen(step);
    se.head.replaceChildren(
      h("span", { class: "caret" }, open ? "▾" : "▸"),
      h("div", { class: "step-cell" },
        h("div", { class: "step-name" }, step.type),
        step.from_types && step.from_types.length
          ? h("div", { class: "step-from", title: "depends on " + step.from_types.join(", ") }, "← " + step.from_types.join(", "))
          : null),
      h("span", { class: "step-count" }, step.total),
      miniTallies(counts),
      h("div", { class: "prog" }, segBar(counts, step.total), h("span", { class: "prog-text" }, `${doneOf(step)}/${step.total}`)));
    if (!open) { se.list.replaceChildren(); se.rows.clear(); se.list.hidden = true; return; }
    se.list.hidden = false;
    if (se.instsHead.parentNode !== se.list) se.list.prepend(se.instsHead);   // header stays first
    renderInstsHead(se, step);
    const origIdx = new Map(step.instances.map((it, n) => [it.relpath, n + 1]));
    const seen = new Set();
    for (const inst of sortedInstances(step)) {
      let row = se.rows.get(inst.relpath);
      if (!row) { row = makeInst(); se.rows.set(inst.relpath, row); }
      patchInst(row, inst, origIdx.get(inst.relpath));
      se.list.append(row);                              // appended after the header, in sort order
      seen.add(inst.relpath);
    }
    for (const [rp, row] of se.rows) if (!seen.has(rp)) { row.remove(); se.rows.delete(rp); }
  }

  function makeInst() {
    return h("a", { class: "inst insts-grid" },
      h("span", { class: "inst-idx" }),
      h("span", { class: "inst-name" }),
      h("span", { class: "inst-status" }, dot("queued"), h("span", { class: "st-text" })),
      h("span", { class: "inst-when" }));
  }

  function patchInst(row, inst, idx) {
    row.className = "inst insts-grid s-" + inst.state + (inst.cached ? " cached" : "");
    if (inst.cached) row.removeAttribute("href");
    else row.href = `#/run/${port}/log/${encodeURIComponent(inst.slug || slugify(inst.relpath))}`;
    row.children[0].textContent = idx;
    row.children[1].textContent = inst.name;
    row.children[1].title = inst.relpath;
    const status = row.children[2];
    status.children[0].style.background = stateColor(inst.state);
    let text = inst.cached ? "cached" : stateLabel(inst.state) + (inst.held ? " · held" : "");
    if (!inst.cached && inst.gpus && inst.gpus.length) text += " · gpu " + inst.gpus.join(",");
    status.children[1].textContent = text;
    row.children[3].textContent = inst.cached ? "" : elapsedText(inst);
    row.title = inst.reason || inst.relpath;
  }

  return { el, setData, render };
}

function countsOf(step) {
  const c = {};
  for (const i of step.instances) c[i.state] = (c[i.state] || 0) + 1;
  return c;
}

// --------------------------------------------------------------------------- //
// Runs (#/) — stat cards + filterable, paginated table
// --------------------------------------------------------------------------- //
const PAGE_SIZE = 12;

function Dashboard(root) {
  setNav("runs"); setCrumbs([]);
  let status = "all", query = "", page = 1, sig = null;

  const sub = h("span", { class: "sub" });
  const stats = h("div", { class: "stats" });
  const statusSel = h("select", { class: "field", onchange: () => { status = statusSel.value; page = 1; draw(true); } },
    ...[["all", "All statuses"], ["live", "Live"], ["completed", "Completed"], ["failed", "Failed"], ["interrupted", "Interrupted"]]
      .map(([v, label]) => h("option", { value: v }, label)));
  const searchInput = h("input", { type: "search", placeholder: "Search runs" });
  searchInput.addEventListener("input", () => { query = searchInput.value.trim().toLowerCase(); page = 1; draw(true); });
  const toolbar = h("div", { class: "toolbar" },
    statusSel,
    h("span", { class: "grow" }),
    h("label", { class: "field search" }, iconSearch(), searchInput));
  const tableWrap = h("div", { class: "table-wrap" });

  root.append(h("div", { class: "page-head" }, h("h1", {}, "Runs"), sub), stats, toolbar, tableWrap);

  const visible = (runs) => runs.filter((r) => {
    if (status !== "all" && r.status !== status) return false;
    if (query) return (r.project || "run").toLowerCase().includes(query) || String(r.port).includes(query);
    return true;
  });

  function draw(force) {
    const runs = Runs.data.runs || [];
    sub.textContent = runs.length ? `${runs.length} run${runs.length > 1 ? "s" : ""}` : "";
    renderStats(stats, runs);
    const shown = visible(runs);
    const pages = Math.max(1, Math.ceil(shown.length / PAGE_SIZE));
    if (page > pages) page = pages;
    const slice = shown.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);
    const next = JSON.stringify([status, query, page, pages, shown.length,
      slice.map((r) => [r.port, r.status, Math.round(r.elapsed || 0), r.counts])]);
    if (!force && next === sig) return;
    sig = next;
    if (!runs.length) return tableWrap.replaceChildren(emptyState());
    if (!shown.length) return tableWrap.replaceChildren(h("div", { class: "empty small" }, "No runs match."));
    const kids = [runsTable(slice)];
    if (pages > 1) kids.push(pager(page, pages, shown.length, (n) => { page = n; draw(true); }));
    tableWrap.replaceChildren(...kids);
  }

  const unsub = Runs.subscribe(() => draw(false));
  return { destroy: unsub };
}

function renderStats(el, runs) {
  const live = runs.filter((r) => r.live).length;
  const completed = runs.filter((r) => r.status === "completed").length;
  const failed = runs.filter((r) => r.status === "failed").length;
  el.replaceChildren(
    statCard("Total runs", runs.length),
    statCard("Active", live, live ? "green" : "muted"),
    statCard("Completed", completed),
    statCard("Failed", failed, failed ? "red" : "muted"));
}

function runsTable(rows) {
  return h("table", { class: "runs" },
    h("thead", {}, h("tr", {},
      h("th", {}, "Run"), h("th", {}, "Status"), h("th", {}, "Progress"),
      h("th", {}, "Started"), h("th", {}, "Duration"))),
    h("tbody", {}, ...rows.map(runRow)));
}

function runRow(r) {
  const c = r.counts || {}, total = r.n_artifacts || 0;
  const done = (c.completed || 0) + (c.cached || 0);
  return h("tr", { onclick: () => { location.hash = `#/run/${r.port}`; } },
    h("td", {}, h("div", { class: "run-name" }, r.project || "run"), h("div", { class: "run-name-sub" }, `:${r.port}`)),
    h("td", {}, pill(r.status)),
    h("td", {}, h("div", { class: "prog" }, segBar(c, total), h("span", { class: "prog-text" }, `${done}/${total}`))),
    h("td", { class: "nowrap muted" }, fmtAgo(r.started_at)),
    h("td", { class: "nowrap muted num" }, fmtDur(r.elapsed)));
}

function pager(page, pages, count, go) {
  const el = h("div", { class: "pager" }, h("span", { class: "count" }, `${count} run${count === 1 ? "" : "s"}`));
  const btn = (label, n, disabled, on) =>
    h("span", { class: "pg" + (disabled ? " disabled" : "") + (on ? " on" : ""), onclick: () => { if (!disabled) go(n); } }, label);
  el.append(btn("‹", page - 1, page <= 1));
  for (const n of pageList(page, pages)) el.append(n === "…" ? h("span", { class: "pg disabled" }, "…") : btn(String(n), n, false, n === page));
  el.append(btn("›", page + 1, page >= pages));
  return el;
}

function pageList(page, pages) {
  if (pages <= 7) return Array.from({ length: pages }, (_, i) => i + 1);
  const keep = [...new Set([1, page - 1, page, page + 1, pages])].filter((n) => n >= 1 && n <= pages).sort((a, b) => a - b);
  const out = []; let prev = 0;
  for (const n of keep) { if (n - prev > 1) out.push("…"); out.push(n); prev = n; }
  return out;
}

// --------------------------------------------------------------------------- //
// All runs (#/all) — every run expanded inline, polled
// --------------------------------------------------------------------------- //
function Overview(root) {
  setNav("all"); setCrumbs([]);
  let query = "", last = { runs: [] };
  const sub = h("span", { class: "sub" });
  const searchInput = h("input", { type: "search", placeholder: "Search runs" });
  searchInput.addEventListener("input", () => { query = searchInput.value.trim().toLowerCase(); render(last); });
  const toolbar = h("div", { class: "toolbar" }, h("span", { class: "grow" }), h("label", { class: "field search" }, iconSearch(), searchInput));
  const list = h("div", { class: "run-list" });
  root.append(h("div", { class: "page-head" }, h("h1", {}, "All runs"), sub), toolbar, list);

  const blocks = new Map();
  let timer = 0, dead = false;
  const match = (r) => !query || (r.project || "run").toLowerCase().includes(query) || String(r.port).includes(query);

  function render(data) {
    last = data;
    const all = data.runs || [];
    const runs = all.filter(match);
    sub.textContent = all.length ? `${all.length} run${all.length > 1 ? "s" : ""}` : "";
    if (!all.length) { list.replaceChildren(emptyState()); blocks.clear(); return; }
    if (!runs.length) { list.replaceChildren(h("div", { class: "empty small" }, "No runs match.")); blocks.clear(); return; }
    if (list.firstChild && list.firstChild.classList && list.firstChild.classList.contains("empty")) list.firstChild.remove();
    const seen = new Set();
    for (const r of runs) {
      let b = blocks.get(r.port);
      if (!b) { b = makeRunBlock(r.port); blocks.set(r.port, b); }
      patchRunBlock(b, r);
      list.append(b.block);
      seen.add(r.port);
    }
    for (const [p, b] of blocks) if (!seen.has(p)) { b.block.remove(); blocks.delete(p); }
  }

  async function tick() {
    if (dead) return;
    try { render(await api("/api/overview")); timer = setTimeout(tick, 2000); }
    catch { timer = setTimeout(tick, 3000); }
  }
  tick();
  return { destroy() { dead = true; clearTimeout(timer); } };
}

function makeRunBlock(port) {
  const head = h("a", { class: "run-head", href: `#/run/${port}` });
  const steps = StepsView(port, { header: false });
  return { block: h("div", { class: "run-block" }, head, steps.el), head, steps };
}

function patchRunBlock(b, r) {
  const c = r.counts || {}, total = r.n_artifacts || 0;
  const done = (c.completed || 0) + (c.cached || 0);
  b.head.replaceChildren(
    h("span", { class: "run-title" }, r.project || "run"),
    h("span", { class: "run-port" }, `:${r.port}`),
    pill(r.status),
    h("span", { class: "run-sub" }, `${done}/${total} done · ${fmtDur(r.elapsed)}`),
    h("span", { class: "spacer" }),
    miniTallies(c),
    segBar(c, total, "mini"));
  b.steps.setData(r.steps || []);
}

// --------------------------------------------------------------------------- //
// Run detail (#/run/<port>) — polls the full snapshot on an interval
// --------------------------------------------------------------------------- //
function RunDetail(root, port) {
  setNav("runs"); setCrumbs([{ label: `run :${port}` }]);
  const head = h("div", { class: "page-head" });
  const stats = h("div", { class: "stats" });
  const gauges = h("div", { class: "gauges" });
  const overall = h("div", { class: "overall" });
  const stepsView = StepsView(port);
  root.append(head, stats, gauges, overall, stepsView.el);

  const M = { status: "", live: false, started_at: null, ended_at: null, project: null,
              pool: {}, steps: [] };
  let raf = 0, dead = false, timer = 0;

  function applySnapshot(d) {
    Object.assign(M, {
      status: d.status, live: d.live, started_at: d.started_at, ended_at: d.ended_at,
      project: d.project, pool: d.pool || {}, steps: d.steps || [],
    });
    setCrumbs([{ label: (d.project ? d.project + " " : "") + `:${port}` }]);
    render();
  }

  const scheduleRender = () => { if (!raf) raf = requestAnimationFrame(() => { raf = 0; render(); }); };

  function render() {
    if (dead) return;
    const elapsed = (M.ended_at != null ? M.ended_at : serverNow()) - (M.started_at || serverNow());
    head.replaceChildren(
      h("h1", {}, M.project || "run"),
      pill(M.status),
      h("span", { class: "sub" }, `${M.live ? "running" : M.status} · ${fmtDur(elapsed)} · started ${fmtAgo(M.started_at)}`));

    const counts = allCounts();
    const total = M.steps.reduce((n, s) => n + s.total, 0);
    const running = (counts.running || 0) + (counts.yielding || 0);
    const done = (counts.completed || 0) + (counts.cached || 0);
    const failed = (counts.failed || 0) + (counts.cancelled || 0);
    stats.replaceChildren(
      statCard("Artifacts", total),
      statCard("Running", running, running ? "green" : "muted"),
      statCard("Done", done),
      statCard("Failed", failed, failed ? "red" : "muted"));

    gauges.replaceChildren(...gaugeEls(M.pool));

    overall.replaceChildren(
      h("div", { class: "overall-line" },
        h("span", { class: "overall-label" }, `${done} / ${total} done`),
        tallies(counts)),
      segBar(counts, total));

    stepsView.setData(M.steps);
  }

  function allCounts() {
    const c = {};
    for (const st of M.steps) for (const i of st.instances) c[i.state] = (c[i.state] || 0) + 1;
    return c;
  }

  // Poll the full snapshot on an interval. This is robust against missed events, dropped
  // connections, and re-runs (a re-run that reuses the port appends to the same log; the server
  // resets its view on each server_start, so re-queued jobs come back as queued). A 1s tick keeps
  // the elapsed clock smooth between polls.
  async function poll() {
    if (dead) return;
    try {
      const d = await api(`/api/runs/${port}`);
      if (d) applySnapshot(d);
    } catch { /* transient — retry next interval */ }
    if (!dead) timer = setTimeout(poll, 2000);
  }
  poll();

  const tick = setInterval(() => { if (M.live) scheduleRender(); }, 1000);

  return { destroy() { dead = true; clearTimeout(timer); clearInterval(tick); } };
}

function gaugeEls(pool) {
  const out = [];
  const p = pool || {};
  if (p.gpus && p.gpus.total) out.push(gauge("GPU", p.gpus.total - p.gpus.free, p.gpus.total, ""));
  if (p.cpus && p.cpus.total) out.push(gauge("CPU", p.cpus.total - p.cpus.free, p.cpus.total, ""));
  if (p.memory_mb && p.memory_mb.total)
    out.push(gauge("Memory", gb(p.memory_mb.total - p.memory_mb.free), gb(p.memory_mb.total), "GB"));
  return out;
}

const gb = (mb) => Math.round((mb / 1024) * 10) / 10;

function gauge(label, used, total, unit) {
  const pct = total ? (100 * used) / total : 0;
  return h("div", { class: "gauge" },
    h("div", { class: "label" }, label),
    h("div", { class: "val" }, `${used}`, h("small", {}, ` / ${total}${unit ? " " + unit : ""}`)),
    h("div", { class: "track" }, h("span", { style: `width:${pct}%` })));
}

// --------------------------------------------------------------------------- //
// Log view (#/run/<port>/log/<slug>) — SSE tail with follow-on-scroll
// --------------------------------------------------------------------------- //
function LogView(root, port, slug) {
  setNav("runs");
  root.className = "wide";
  setCrumbs([{ label: `run :${port}`, href: `#/run/${port}` }, { label: slug }]);

  const title = h("h1", {}, slug);
  const statusEl = h("span", {});
  const metaEl = h("span", { class: "sub" });
  const wrap = checkbox("wrap", false);
  const follow = checkbox("follow", true);
  const raw = h("a", { href: `/api/runs/${port}/log/${encodeURIComponent(slug)}`, target: "_blank" }, "raw");
  const pathEl = h("div", { class: "log-path" });
  const pre = h("pre", { class: "log", tabindex: "0" }, h("span", { class: "placeholder" }, "Connecting…"));
  const jump = h("button", { class: "jump" }, "Jump to bottom ↓");

  root.append(
    h("div", { class: "log-head" },
      h("a", { href: `#/run/${port}` }, "‹ back"), title, statusEl, metaEl,
      h("div", { class: "right" }, wrap.label, follow.label, raw)),
    pathEl,
    h("div", { class: "log-wrap" }, pre, jump));

  let lines = [], tail = "", following = true, started = false, raf = 0, dead = false;
  const collapse = (s) => { const i = s.lastIndexOf("\r"); return i >= 0 ? s.slice(i + 1) : s; };
  const nearBottom = () => pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 24;

  function render() {
    if (dead) return;
    const text = lines.join("\n") + (tail ? (lines.length ? "\n" : "") + collapse(tail) : "");
    if (text) pre.textContent = text;
    else pre.replaceChildren(h("span", { class: "placeholder" }, started ? "(no output)" : "Connecting…"));
    if (following) pre.scrollTop = pre.scrollHeight;
    jump.classList.toggle("show", !following);
  }
  const scheduleRender = () => { if (!raf) raf = requestAnimationFrame(() => { raf = 0; render(); }); };

  function append(chunk) {
    started = true;
    tail += chunk;
    const parts = tail.split("\n");
    tail = parts.pop();
    for (const p of parts) lines.push(collapse(p));
    scheduleRender();
  }

  const es = new EventSource(`/api/runs/${port}/log/${encodeURIComponent(slug)}/stream`);
  es.onmessage = (e) => append(JSON.parse(e.data));
  es.addEventListener("reset", () => { lines = []; tail = ""; scheduleRender(); });
  es.addEventListener("end", () => { es.close(); started = true; scheduleRender(); });

  pre.addEventListener("scroll", () => { following = nearBottom(); follow.input.checked = following; jump.classList.toggle("show", !following); });
  follow.input.addEventListener("change", () => { following = follow.input.checked; if (following) pre.scrollTop = pre.scrollHeight; jump.classList.toggle("show", !following); });
  wrap.input.addEventListener("change", () => pre.classList.toggle("wrap", wrap.input.checked));
  jump.addEventListener("click", () => { following = true; follow.input.checked = true; pre.scrollTop = pre.scrollHeight; jump.classList.remove("show"); });

  // Resolve header (name, state, path) once, then keep it fresh while the job runs.
  let infoTimer = 0;
  async function info() {
    if (dead) return;
    try {
      const d = await api(`/api/runs/${port}`);
      const inst = findInstance(d, slug);
      if (inst) {
        title.textContent = inst.name || slug;
        statusEl.replaceChildren(statePill(inst.state));
        metaEl.textContent = elapsedText(inst);
        setCrumbs([{ label: (d.project ? d.project + " " : "") + `:${port}`, href: `#/run/${port}` }, { label: inst.name || slug }]);
      }
      if (d.logdir) pathEl.textContent = `${d.logdir}/jobs/${slug}.log`;
      const running = d.status === "live" && (!inst || !TERMINAL.has(inst.state));
      if (running) infoTimer = setTimeout(info, 2000);
    } catch {
      if (!dead) infoTimer = setTimeout(info, 4000);
    }
  }
  info();

  return { destroy() { dead = true; es.close(); clearTimeout(infoTimer); } };
}

function findInstance(detail, slug) {
  for (const st of detail.steps || [])
    for (const inst of st.instances)
      if (inst.slug === slug || slugify(inst.relpath) === slug) return inst;
  return null;
}

function checkbox(name, checked) {
  const input = h("input", { type: "checkbox", ...(checked ? { checked: true } : {}) });
  return { input, label: h("label", { class: "toggle" }, input, name) };
}

// --------------------------------------------------------------------------- //
// Router + boot
// --------------------------------------------------------------------------- //
const main = document.getElementById("view");
let active = null;

function route() {
  if (active && active.destroy) active.destroy();
  active = null;
  main.className = "";
  main.replaceChildren();
  const parts = (location.hash.replace(/^#/, "") || "/").split("/").filter(Boolean);
  if (parts[0] === "run" && parts[1]) {
    if (parts[2] === "log" && parts[3]) active = LogView(main, parts[1], decodeURIComponent(parts[3]));
    else active = RunDetail(main, parts[1]);
  } else if (parts[0] === "all") {
    active = Overview(main);
  } else {
    active = Dashboard(main);
  }
}

function initHeader() {
  const conn = document.getElementById("conn");
  const footDot = document.getElementById("foot-dot");
  const footText = document.getElementById("foot-text");
  Runs.subscribe((data, ok) => {
    const runs = data.runs || [];
    const live = runs.filter((r) => r.live).length;
    conn.classList.toggle("active", live > 0);
    conn.classList.toggle("disconnected", ok === false);
    conn.replaceChildren(h("span", { class: "pulse" }),
      h("span", {}, ok === false ? "disconnected" : live ? `${live} active` : "idle"));
    if (footDot) footDot.classList.toggle("live", live > 0);
    if (footText) footText.textContent = ok === false ? "disconnected" : runs.length ? `${runs.length} run${runs.length === 1 ? "" : "s"}` : "watching runs";
  });
}

window.addEventListener("hashchange", route);
initHeader();
Runs.start();
route();
