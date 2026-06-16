"use strict";
// pipelines dashboard — a small single-page app. No framework, no build step.
//
//   • a left sidebar + a shared poll of /api/runs drive navigation and the header
//   • Runs (#/)  a stat-card summary + runs grouped by name into collapsible tables (history kept)
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

const STATES = ["running", "yielding", "queued", "held", "blocked", "completed", "cached", "skipped", "failed", "cancelled"];
const TERMINAL = new Set(["completed", "cached", "skipped", "failed", "cancelled", "blocked"]);
const isActive = (s) => s === "running" || s === "yielding";

// Default order for a step's expanded instances: active on top, cached/skipped at the bottom.
const INST_RANK = { running: 0, yielding: 1, queued: 2, held: 3, blocked: 4, failed: 5, cancelled: 6, completed: 7, cached: 8, skipped: 9 };

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

// Absolute local start time (MM-DD HH:MM) — disambiguates re-runs of the same name in a group.
function fmtDateTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000), p = (n) => String(n).padStart(2, "0");
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
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
  if (r.ok) { noteAllowed(); return r.json(); }
  if (r.status === 403) noteForbidden();              // a reachable server refusing us, not a dead one
  const err = new Error(`${path}: ${r.status}`);
  err.status = r.status;
  throw err;
}

// --------------------------------------------------------------------------- //
// Recovering a *forbidden* connection (HTTP 403).
//
// 403 isn't a dead server — it's a reachable one refusing us. On a VS Code / dev-tunnel
// port-forward that means the auth cookie lapsed (typically when the server bounced and the
// tunnel was re-established). fetch / EventSource can't redo that handshake; only a full
// document navigation can. So when 403s persist we reload once or twice to re-auth, then fall
// back to a clickable "reload" badge — a guard against trapping the tab in a reload loop when a
// port is genuinely forbidden. Any successful request clears the state. (A plain dead server
// rejects the fetch outright or answers 5xx, so it never trips this path — it reads as offline.)
// --------------------------------------------------------------------------- //
const REAUTH_KEY = "pipelines.reauth";
let forbidden = false, forbiddenSince = 0;

function connState() {
  if (forbidden) return "forbidden";
  if (Runs.ok === false) return "offline";
  return "online";
}

function noteAllowed() {
  if (forbidden || forbiddenSince) {
    forbidden = false; forbiddenSince = 0;
    try { sessionStorage.removeItem(REAUTH_KEY); } catch { /* storage blocked (private mode) */ }
  }
}

function noteForbidden() {
  if (!forbiddenSince) forbiddenSince = Date.now();
  if (Date.now() - forbiddenSince < 5000) return;     // ride out a brief blip while the server restarts
  if (reauthReload()) return;                          // guarded full reload re-runs the proxy handshake
  forbidden = true;                                    // gave up auto-reloading — surface a manual control
}

function reauthReload() {
  let st;
  try { st = JSON.parse(sessionStorage.getItem(REAUTH_KEY) || "{}"); } catch { st = {}; }
  const now = Date.now();
  if ((st.n || 0) >= 2) return false;                  // already reloaded twice this session — give up
  if (st.at && now - st.at < 10000) return false;      // and never reload faster than every 10s
  try { sessionStorage.setItem(REAUTH_KEY, JSON.stringify({ n: (st.n || 0) + 1, at: now })); } catch { /**/ }
  location.reload();
  return true;
}

function manualReconnect() {
  try { sessionStorage.removeItem(REAUTH_KEY); } catch { /* storage blocked (private mode) */ }
  location.reload();
}

// --------------------------------------------------------------------------- //
// Shared run poll — feeds the header always, and the dashboard while mounted.
// Also tracks the server/client clock offset so elapsed times survive skew.
// --------------------------------------------------------------------------- //
let clockOffset = 0, lastClockSync = 0;
const serverNow = () => Date.now() / 1000 + clockOffset;

// Re-calibrate the client→server clock offset at most every 30s. Between syncs the elapsed clock
// advances purely off the local Date.now(), so it ticks smoothly instead of jittering on each poll
// as the offset is nudged.
function syncClock(serverTime) {
  const nowMs = Date.now();
  if (nowMs - lastClockSync >= 30000) {
    clockOffset = (serverTime || nowMs / 1000) - nowMs / 1000;
    lastClockSync = nowMs;
  }
}

// --------------------------------------------------------------------------- //
// Adaptive polling: ~2s while the window is in use, easing linearly to 60s after
// ~10 min of inactivity; returning from idle kicks the live loops to refresh at once.
// A cycle that fails (server down / moved) ignores the idle ramp and retries on the
// short RECONNECT_POLL cadence, so we notice the server is back within a few seconds.
// --------------------------------------------------------------------------- //
const MIN_POLL = 2000, MAX_POLL = 60000, RECONNECT_POLL = 3000, IDLE_RAMP_MS = 10 * 60 * 1000;
let lastActivity = Date.now();
const pollLoops = new Set();

function pollDelay() {
  const f = Math.min(1, (Date.now() - lastActivity) / IDLE_RAMP_MS);
  return MIN_POLL + (MAX_POLL - MIN_POLL) * f;
}

function adaptiveLoop(fn) {
  let timer = 0, stopped = false, busy = false, failed = false;
  async function cycle() {
    busy = true;
    try { await fn(); failed = false; } catch { failed = true; }   // offline → retry fast, not on the idle ramp
    busy = false;
    if (!stopped) timer = setTimeout(cycle, failed ? RECONNECT_POLL : pollDelay());
  }
  const ctl = {
    cancel() { stopped = true; clearTimeout(timer); pollLoops.delete(ctl); },
    kick() { if (!stopped && !busy) { clearTimeout(timer); cycle(); } },
  };
  pollLoops.add(ctl);
  cycle();
  return ctl;
}

function markActivity() {
  const resumed = Date.now() - lastActivity > 4000;   // we'd eased off → refresh promptly
  lastActivity = Date.now();
  if (resumed) for (const l of pollLoops) l.kick();
}

const Runs = {
  data: { runs: [], now: 0 },
  ok: null,
  subs: new Set(),
  loop: null,
  start() {
    if (this.loop) return;
    this.loop = adaptiveLoop(() => this.tick());
  },
  async tick() {
    try {
      this.data = await api("/api/runs");
      syncClock(this.data.now);
      this.ok = true;
    } catch (e) {
      this.ok = false;
      throw e;                 // re-thrown so adaptiveLoop retries on the fast reconnect cadence
    } finally {
      for (const fn of this.subs) fn(this.data, this.ok);
    }
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

// Shown in place of a run's status pill when the poll can't reach the server (or is being refused).
// The poll keeps retrying underneath, so this clears itself the moment the server answers again.
function disconnectedPill(text) {
  return h("span", { class: "pill disconnected" }, dot("failed"), text || "disconnected");
}

// A run's status pill, connection-aware: a live run shown while the server is unreachable reads
// "disconnected" (or "blocked" on a 403) instead of a stale, still-pulsing "live". Terminal runs
// (completed / failed / interrupted) are settled, so they keep their status regardless.
function runStatusPill(r) {
  const cs = connState();
  if ((r.live || r.status === "live") && cs !== "online")
    return disconnectedPill(cs === "forbidden" ? "blocked" : "disconnected");
  return pill(r.status);
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

  const doneOf = (s) => s.instances.reduce((n, i) => n + (i.state === "completed" || i.state === "cached" || i.state === "skipped" ? 1 : 0), 0);
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
    // Cached and (transient) skipped instances never ran this session: no log to link, no elapsed.
    const noRun = inst.cached || inst.skipped;
    row.className = "inst insts-grid s-" + inst.state + (inst.cached ? " cached" : inst.skipped ? " skipped" : "");
    if (noRun) row.removeAttribute("href");
    else row.href = `#/run/${port}/log/${encodeURIComponent(inst.slug || slugify(inst.relpath))}`;
    row.children[0].textContent = idx;
    row.children[1].textContent = inst.name;
    row.children[1].title = inst.relpath;
    const status = row.children[2];
    status.children[0].style.background = stateColor(inst.state);
    let text = inst.cached ? "cached" : inst.skipped ? "skipped" : stateLabel(inst.state) + (inst.held ? " · held" : "");
    if (!noRun && inst.gpus && inst.gpus.length) text += " · gpu " + inst.gpus.join(",");
    status.children[1].textContent = text;
    row.children[3].textContent = noRun ? "" : elapsedText(inst);
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
// Runs (#/) — stat cards + runs grouped by name (project), newest first.
// Each name is a collapsible table of its runs, so re-running keeps prior runs
// as history (their status + progress) instead of replacing them.
// --------------------------------------------------------------------------- //
function Dashboard(root, focus) {
  setNav("runs"); setCrumbs([]);
  let status = "all", query = "", sig = null;
  const expanded = new Map();   // group name -> explicit open bool (user choice)
  const toggled = new Set();    // groups the user has decided about (overrides the auto rule)
  if (focus) { toggled.add(focus); expanded.set(focus, true); }   // arrived from a sidebar group
  let scrolledToFocus = false;

  const sub = h("span", { class: "sub" });
  const stats = h("div", { class: "stats" });
  const statusSel = h("select", { class: "field", onchange: () => { status = statusSel.value; draw(true); } },
    ...[["all", "All statuses"], ["live", "Live"], ["completed", "Completed"], ["failed", "Failed"], ["interrupted", "Interrupted"]]
      .map(([v, label]) => h("option", { value: v }, label)));
  const searchInput = h("input", { type: "search", placeholder: "Search runs" });
  searchInput.addEventListener("input", () => { query = searchInput.value.trim().toLowerCase(); draw(true); });
  const toolbar = h("div", { class: "toolbar" },
    statusSel,
    h("span", { class: "grow" }),
    h("label", { class: "field search" }, iconSearch(), searchInput));
  const groupsWrap = h("div", { class: "run-groups" });

  root.append(h("div", { class: "page-head" }, h("h1", {}, "Runs"), sub), stats, toolbar, groupsWrap);

  const groupEls = new Map();   // name -> { block, head, caret, summary, body, tbody, rows, runs, open }

  const visible = (runs) => runs.filter((r) => {
    if (status !== "all" && r.status !== status) return false;
    if (query) return (r.project || "run").toLowerCase().includes(query)
      || (r.node || "").toLowerCase().includes(query) || String(r.port).toLowerCase().includes(query);
    return true;
  });

  // A group is open if the user toggled it; else automatically while it has a live run, and for the
  // most-recently-active group (idx 0) so the latest experiment's runs show without a click.
  const isOpen = (name, runs, idx) =>
    toggled.has(name) ? !!expanded.get(name) : (idx === 0 || runs.some((r) => r.live));

  function draw(force) {
    const runs = Runs.data.runs || [];
    sub.textContent = runs.length ? `${runs.length} run${runs.length > 1 ? "s" : ""}` : "";
    renderStats(stats, runs);
    const groups = groupRuns(visible(runs));
    const next = JSON.stringify([connState(), status, query, groups.map((g, i) =>
      [g.name, isOpen(g.name, g.runs, i),
       g.runs.map((r) => [r.port, r.status, r.live, Math.round(r.elapsed || 0),
         (r.counts || {}).completed, (r.counts || {}).failed])])]);
    if (!force && next === sig) return;
    sig = next;
    if (!runs.length) { groupsWrap.replaceChildren(emptyState()); groupEls.clear(); return; }
    if (!groups.length) { groupsWrap.replaceChildren(h("div", { class: "empty small" }, "No runs match.")); groupEls.clear(); return; }
    if (groupsWrap.firstChild && groupsWrap.firstChild.classList && groupsWrap.firstChild.classList.contains("empty"))
      groupsWrap.firstChild.remove();
    const seen = new Set();
    groups.forEach((g, i) => {
      let ge = groupEls.get(g.name);
      if (!ge) { ge = makeGroup(g.name); groupEls.set(g.name, ge); }
      patchGroup(ge, g, i);
      groupsWrap.append(ge.block);              // (re)order in place; moves existing nodes, no flicker
      seen.add(g.name);
    });
    for (const [n, ge] of groupEls) if (!seen.has(n)) { ge.block.remove(); groupEls.delete(n); }
    if (focus && !scrolledToFocus && groupEls.has(focus)) {   // bring the picked group into view once
      groupEls.get(focus).block.scrollIntoView({ block: "nearest" });
      scrolledToFocus = true;
    }
  }

  function makeGroup(name) {
    const ge = { name, rows: new Map() };
    ge.caret = h("span", { class: "caret" });
    ge.summary = h("span", { class: "group-summary" });
    ge.head = h("div", { class: "group-head", onclick: () => toggleGroup(ge) },
      ge.caret, h("span", { class: "group-name" }, name), h("span", { class: "spacer" }), ge.summary);
    ge.tbody = h("tbody", {});
    ge.body = h("div", { class: "group-body" }, h("table", { class: "runs grouped" }, ge.tbody));
    ge.block = h("div", { class: "run-group" }, ge.head, ge.body);
    return ge;
  }

  function toggleGroup(ge) {
    toggled.add(ge.name);
    expanded.set(ge.name, !ge.open);            // ge.open is the state shown right now
    draw(true);
  }

  function patchGroup(ge, g, idx) {
    ge.runs = g.runs;
    ge.open = isOpen(g.name, g.runs, idx);
    ge.caret.textContent = ge.open ? "▾" : "▸";
    ge.summary.replaceChildren(...groupSummary(g.runs));
    ge.body.hidden = !ge.open;
    const seen = new Set();
    for (const r of g.runs) {
      let row = ge.rows.get(r.port);
      if (!row) { row = makeRunRow(); ge.rows.set(r.port, row); }
      patchRunRow(row, r);
      ge.tbody.append(row);                     // (re)order rows in place
      seen.add(r.port);
    }
    for (const [p, row] of ge.rows) if (!seen.has(p)) { row.remove(); ge.rows.delete(p); }
  }

  const unsub = Runs.subscribe(() => draw(false));
  return { destroy: unsub };
}

// Bucket runs by name (project); newest run first within each; live groups first, then by recency.
function groupRuns(runs) {
  const by = new Map();
  for (const r of runs) {
    const name = r.project || "run";
    let arr = by.get(name);
    if (!arr) { arr = []; by.set(name, arr); }
    arr.push(r);
  }
  const groups = [];
  for (const [name, rs] of by) {
    rs.sort((a, b) => (b.started_at || 0) - (a.started_at || 0));
    groups.push({ name, runs: rs });
  }
  groups.sort((a, b) => {
    const al = a.runs.some((r) => r.live), bl = b.runs.some((r) => r.live);
    if (al !== bl) return al ? -1 : 1;
    return (b.runs[0].started_at || 0) - (a.runs[0].started_at || 0);
  });
  return groups;
}

// A group's overall status, connection-aware: live (or disconnected/blocked while unreachable),
// else the latest run's outcome (completed / failed / interrupted). Used by the runs list + sidebar,
// so the status (Completed / Failed / Live / disconnected) shows wherever experiments are listed.
function groupStatusInfo(runs) {
  const liveN = runs.filter((r) => r.live).length;
  const cs = connState();
  if (liveN && cs !== "online")
    return { label: cs === "forbidden" ? "blocked" : "disconnected", tone: "failed", live: false, liveN };
  if (liveN) return { label: liveN > 1 ? `${liveN} live` : "live", tone: "running", live: true, liveN };
  const st = runs[0] ? runs[0].status : "";       // newest run first → its outcome is the group's
  const tone = st === "completed" ? "completed" : st === "failed" ? "failed" : "cancelled";
  return { label: st || "—", tone, live: false, liveN: 0 };
}

// Header summary for a name: the group's status pill + run count + ✓ / ✗ tallies.
function groupSummary(runs) {
  const info = groupStatusInfo(runs);
  const completed = runs.filter((r) => r.status === "completed").length;
  const failed = runs.filter((r) => r.status === "failed").length;
  const primary = info.live
    ? h("span", { class: "pill live" }, dot("running"), info.label)
    : (info.label === "disconnected" || info.label === "blocked")
      ? disconnectedPill(info.label)
      : pill(runs[0] ? runs[0].status : "interrupted");
  const tail = [];
  if (completed) tail.push(`${completed} ✓`);
  if (failed) tail.push(`${failed} ✗`);
  return [primary, h("span", { class: "group-meta" },
    `${runs.length} run${runs.length === 1 ? "" : "s"}` + (tail.length ? " · " + tail.join(" · ") : ""))];
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

// One run row inside a group's table. Keyed by run id (r.port) so live updates patch in place.
function makeRunRow() {
  return h("tr", {},
    h("td", {}), h("td", {}), h("td", {}),
    h("td", { class: "nowrap muted" }), h("td", { class: "nowrap muted num" }));
}

function patchRunRow(tr, r) {
  tr.onclick = () => { location.hash = `#/run/${r.port}`; };
  const c = r.counts || {}, total = r.n_artifacts || 0;
  const done = (c.completed || 0) + (c.cached || 0) + (c.skipped || 0);
  tr.children[0].replaceChildren(
    h("div", { class: "run-name" }, r.node || "run"),                  // which run: the host it ran on
    h("div", { class: "run-name-sub" }, fmtDateTime(r.started_at)));   // …and when it started
  tr.children[1].replaceChildren(runStatusPill(r));
  tr.children[2].replaceChildren(h("div", { class: "prog" }, segBar(c, total), h("span", { class: "prog-text" }, `${done}/${total}`)));
  tr.children[3].textContent = fmtAgo(r.started_at);
  tr.children[4].textContent = fmtDur(r.elapsed);
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
  let dead = false;
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

  const loop = adaptiveLoop(async () => {
    if (dead) return;
    const d = await api("/api/overview");
    if (!dead) render(d);
  });
  // Re-render on connection changes so a live run reads "disconnected" the instant the server drops.
  const unsubConn = Runs.subscribe(() => { if (!dead) render(last); });
  return { destroy() { dead = true; loop.cancel(); unsubConn(); } };
}

function makeRunBlock(port) {
  const head = h("a", { class: "run-head", href: `#/run/${port}` });
  const steps = StepsView(port, { header: false });
  return { block: h("div", { class: "run-block" }, head, steps.el), head, steps };
}

function patchRunBlock(b, r) {
  const c = r.counts || {}, total = r.n_artifacts || 0;
  const done = (c.completed || 0) + (c.cached || 0) + (c.skipped || 0);
  b.head.replaceChildren(
    h("span", { class: "run-title" }, r.project || "run"),
    h("span", { class: "run-port" }, r.node ? `${r.node} · :${r.port}` : `:${r.port}`),
    runStatusPill(r),
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
  let raf = 0, dead = false;

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
    // connState() is the shared "can we reach the server" signal. While it isn't "online" we can't
    // trust the last snapshot, so the status reads disconnected/blocked rather than a stale,
    // still-pulsing "live". 403 ("blocked") needs a reload to re-auth; the topbar badge is the control.
    const cs = connState();
    const elapsed = (M.ended_at != null ? M.ended_at : serverNow()) - (M.started_at || serverNow());
    head.replaceChildren(
      h("h1", {}, M.project || "run"),
      cs !== "online" ? disconnectedPill(cs === "forbidden" ? "blocked" : "disconnected") : pill(M.status),
      h("span", { class: "sub" },
        cs === "forbidden" ? "connection blocked (403) — reload to reconnect"
        : cs === "offline" ? "lost connection to server — reconnecting…"
        : `${M.live ? "running" : M.status} · ${fmtDur(elapsed)} · started ${fmtAgo(M.started_at)}`));

    const counts = allCounts();
    const total = M.steps.reduce((n, s) => n + s.total, 0);
    const running = (counts.running || 0) + (counts.yielding || 0);
    const done = (counts.completed || 0) + (counts.cached || 0) + (counts.skipped || 0);
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

  // Poll the full snapshot on the shared adaptive interval — robust against missed events, dropped
  // connections, and re-runs (a re-run that reuses the port appends to the same log; the server
  // resets its view on each server_start, so re-queued jobs come back as queued).
  const loop = adaptiveLoop(async () => {
    if (dead) return;
    const d = await api(`/api/runs/${port}`);
    if (!dead && d) applySnapshot(d);
  });

  // A 1s client tick keeps the elapsed clock smooth between polls (cheap, local; the clock offset
  // re-syncs at most every 30s, so it advances without jitter). Skipped while the tab is hidden.
  const tick = setInterval(() => { if (!document.hidden && M.live) scheduleRender(); }, 1000);

  // Reflect the shared connection signal: flip to "disconnected" / back to live the instant the
  // heartbeat poll changes, without waiting on this page's own (slower) snapshot poll.
  const unsubConn = Runs.subscribe(() => scheduleRender());

  return { destroy() { dead = true; loop.cancel(); clearInterval(tick); unsubConn(); } };
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

  // The server streams the log from the top on each new connection, then live-tails it. The
  // browser's transparent EventSource retry would replay from the top and duplicate what we
  // already have — so we own reconnection: on error, clear the buffer and re-open from scratch
  // on the reconnect cadence (also covers the server restarting / moving to a new machine).
  const streamUrl = `/api/runs/${port}/log/${encodeURIComponent(slug)}/stream`;
  let es = null, ended = false, reconnectTimer = 0;
  function connect() {
    es = new EventSource(streamUrl);
    es.onmessage = (e) => append(JSON.parse(e.data));
    es.addEventListener("reset", () => { lines = []; tail = ""; scheduleRender(); });
    es.addEventListener("end", () => { ended = true; es.close(); started = true; scheduleRender(); });
    es.onerror = () => {
      if (dead || ended) return;
      es.close();
      lines = []; tail = ""; started = false; scheduleRender();   // clean slate; shows "Connecting…"
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(connect, RECONNECT_POLL);
    };
  }
  connect();

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

  return { destroy() { dead = true; ended = true; if (es) es.close(); clearTimeout(infoTimer); clearTimeout(reconnectTimer); } };
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
// TimeChart — a tiny canvas line chart: time on x, % on y, with a hover readout.
// No dependencies: one <canvas>, redrawn on data / resize / hover. Series carry
// optional per-point `labels` (e.g. GB) shown in the tooltip instead of the raw %.
// --------------------------------------------------------------------------- //
const CHART_FONT = "11px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";

function cssVar(name, fallback) {
  return getComputedStyle(document.body).getPropertyValue(name).trim() || fallback;
}

function fmtClock(t, withSec) {
  const d = new Date(t * 1000), p = (n) => String(n).padStart(2, "0");
  return withSec ? `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
                 : `${p(d.getHours())}:${p(d.getMinutes())}`;
}

function niceCeil(m) {                          // smallest 1/2/5 × 10ⁿ ≥ m — a tidy axis maximum
  if (!(m > 0)) return 1;
  const base = Math.pow(10, Math.floor(Math.log10(m))), n = m / base;
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * base;
}

function fmtRate(bps) {                          // bytes/s → "12.3 MB/s"
  if (bps == null) return "—";
  const u = ["B/s", "KB/s", "MB/s", "GB/s", "TB/s"];
  let v = bps, i = 0;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return (v < 10 && i ? v.toFixed(1) : Math.round(v)) + " " + u[i];
}

function TimeChart(opts = {}) {
  const autoY = opts.ymax === null;              // null ymax → scale the axis to the data
  const baseMax = autoY ? null : (opts.ymax ?? 100);
  const yticks = opts.yticks || 4;
  const fmtY = opts.fmtY || ((v) => Math.round(v) + (opts.unit || "%"));
  const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));

  const canvas = h("canvas", { class: "chart-canvas" });
  const tip = h("div", { class: "chart-tip" });
  const empty = h("div", { class: "chart-empty" }, opts.empty || "No data yet");
  const el = h("div", { class: "chart", style: `height:${opts.height || 190}px` }, canvas, tip, empty);

  let times = [], series = [], range = [0, 1], hoverIdx = null, L = null;

  function setData(d) {
    times = d.times || []; series = d.series || []; range = d.range || [0, 1];
    if (hoverIdx != null && hoverIdx >= times.length) hoverIdx = null;
    draw();
  }

  function draw() {
    const w = el.clientWidth, hgt = el.clientHeight;
    if (!w || !hgt) return;                                   // not laid out yet; ResizeObserver redraws
    if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(hgt * dpr)) {
      canvas.width = Math.round(w * dpr); canvas.height = Math.round(hgt * dpr);
    }
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, hgt);

    const hasData = times.length > 0 && series.some((s) => s.values.some((v) => v != null));
    empty.style.display = hasData ? "none" : "";
    if (!hasData) { L = null; tip.classList.remove("show"); return; }

    const grid = cssVar("--border", "#e9ebef"), axis = cssVar("--faint", "#9aa2af");
    let ymax = baseMax;
    if (ymax == null) {                                       // auto: round the data max up to a nice value
      let m = 0;
      for (const s of series) for (const v of s.values) if (v != null) m = Math.max(m, v);
      ymax = niceCeil(m);
    }
    ctx.font = CHART_FONT; ctx.textBaseline = "middle";
    let ylabW = 0;                                            // size the left gutter to the widest y label
    for (let i = 0; i <= yticks; i++) ylabW = Math.max(ylabW, ctx.measureText(fmtY((ymax * i) / yticks)).width);
    const padL = Math.ceil(ylabW) + 10, padR = 12, padT = 8, padB = 20;
    const plotW = Math.max(1, w - padL - padR), plotH = Math.max(1, hgt - padT - padB);
    const [x0, x1] = range, xspan = Math.max(1, x1 - x0);
    const X = (t) => padL + ((t - x0) / xspan) * plotW;
    const Y = (v) => padT + (1 - v / ymax) * plotH;
    L = { padL, padT, plotW, plotH, X, Y, x0, x1, xspan };

    ctx.lineWidth = 1; ctx.strokeStyle = grid; ctx.fillStyle = axis; ctx.textAlign = "right";
    for (let i = 0; i <= yticks; i++) {                       // horizontal gridlines + y labels
      const v = (ymax * i) / yticks, y = Math.round(Y(v)) + 0.5;
      ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(padL + plotW, y); ctx.stroke();
      ctx.fillText(fmtY(v), padL - 6, Y(v));
    }
    ctx.textAlign = "center";
    const withSec = xspan <= 20 * 60, xticks = 4;
    for (let i = 0; i <= xticks; i++) {                       // vertical gridlines + time labels
      const t = x0 + (xspan * i) / xticks, x = Math.round(X(t)) + 0.5;
      ctx.strokeStyle = grid; ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, padT + plotH); ctx.stroke();
      ctx.fillStyle = axis; ctx.fillText(fmtClock(t, withSec), X(t), padT + plotH + 11);
    }
    ctx.lineWidth = 1.6; ctx.lineJoin = "round"; ctx.lineCap = "round";
    for (const s of series) {                                 // polylines (nulls break the line)
      ctx.strokeStyle = s.color; ctx.beginPath();
      let pen = false;
      for (let i = 0; i < times.length; i++) {
        const v = s.values[i];
        if (v == null) { pen = false; continue; }
        const x = X(times[i]), y = Y(v);
        if (pen) ctx.lineTo(x, y); else { ctx.moveTo(x, y); pen = true; }
      }
      ctx.stroke();
    }
    if (hoverIdx != null && hoverIdx < times.length) drawHover(ctx);
    else tip.classList.remove("show");
  }

  function drawHover(ctx) {
    const t = times[hoverIdx], x = L.X(t);
    ctx.strokeStyle = cssVar("--border-strong", "#dcdfe4"); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(Math.round(x) + 0.5, L.padT); ctx.lineTo(Math.round(x) + 0.5, L.padT + L.plotH); ctx.stroke();
    const rows = [];
    for (const s of series) {
      const v = s.values[hoverIdx];
      if (v == null) continue;
      ctx.fillStyle = s.color;
      ctx.beginPath(); ctx.arc(x, L.Y(v), 3, 0, Math.PI * 2); ctx.fill();
      rows.push({ color: s.color, name: s.name, label: s.labels ? s.labels[hoverIdx] : fmtY(v) });
    }
    tip.replaceChildren(
      h("div", { class: "tip-time" }, fmtClock(t, true)),
      ...rows.map((r) => h("div", { class: "tip-row" },
        h("span", { class: "tip-dot", style: `background:${r.color}` }),
        h("span", { class: "tip-name" }, r.name),
        h("span", { class: "tip-val" }, r.label))));
    tip.classList.add("show");
    const tw = tip.offsetWidth || 130;
    let left = x + 12;
    if (left + tw > el.clientWidth - 4) left = x - tw - 12;
    tip.style.left = Math.max(4, left) + "px";
    tip.style.top = "8px";
  }

  function onMove(e) {
    if (!L || !times.length) return;
    const rect = canvas.getBoundingClientRect();
    const t = L.x0 + ((e.clientX - rect.left - L.padL) / L.plotW) * L.xspan;
    let lo = 0, hi = times.length - 1;                        // nearest sample (times sorted asc)
    while (lo < hi) { const mid = (lo + hi) >> 1; if (times[mid] < t) lo = mid + 1; else hi = mid; }
    let idx = lo;
    if (idx > 0 && Math.abs(times[idx - 1] - t) <= Math.abs(times[idx] - t)) idx -= 1;
    if (idx !== hoverIdx) { hoverIdx = idx; draw(); }
  }

  canvas.addEventListener("pointermove", onMove);
  canvas.addEventListener("pointerleave", () => { if (hoverIdx != null) { hoverIdx = null; draw(); } });
  const ro = new ResizeObserver(() => draw());
  ro.observe(el);
  return { el, setData, draw, destroy() { ro.disconnect(); } };
}

// A titled, collapsible section (caret ▾ open / ▸ hidden — same affordance as a run's steps).
// The header is the toggle; clicks inside `.sys-ctrls` (e.g. the GPU compute/memory switch) pass
// through without collapsing. `onToggle(open)` fires after a user toggle (used to redraw on reveal).
function CollapsibleSection(title, opts = {}) {
  let open = opts.open !== false;
  const caret = h("span", { class: "caret" });
  const titleEl = h("span", { class: "sys-title" }, title);
  const valEl = h("span", { class: "sys-val" });
  const ctrls = h("div", { class: "sys-ctrls" });
  const body = h("div", { class: "sys-body" });
  const head = h("div", { class: "sys-head" }, caret, titleEl, valEl, h("span", { class: "spacer" }), ctrls);
  const el = h("div", { class: "sys-section" }, head, body);
  const apply = () => { caret.textContent = open ? "▾" : "▸"; body.hidden = !open; };
  head.addEventListener("click", (e) => {
    if (e.target.closest(".sys-ctrls")) return;
    open = !open; apply();
    if (opts.onToggle) opts.onToggle(open);
  });
  apply();
  return { el, body, valEl, ctrls, isOpen: () => open };
}

// One chart in a card with no header — the section header above it carries the title + value.
function chartCard(chart) {
  return h("div", { class: "chart-panel bare" }, chart.el);
}

// One GPU's own card: a colored title (matching its line) + a current-value readout + its chart.
function GpuPanel(i) {
  const valEl = h("div", { class: "panel-val" }, "—");
  const titleEl = h("div", { class: "panel-title" },
    h("span", { class: "gpu-dot", style: `background:${gpuColor(i)}` }), "GPU " + i);
  const chart = TimeChart({ height: 158, ymax: 100 });
  const head = h("div", { class: "panel-head" }, titleEl, h("span", { class: "spacer" }), valEl);
  return { el: h("div", { class: "chart-panel" }, head, chart.el), chart, valEl, destroy: () => chart.destroy() };
}

// --------------------------------------------------------------------------- //
// System (#/system[/<node>]) — per-node GPU/CPU/memory time-series.
// Polls /api/system/<node> incrementally (since_ts) and keeps a trailing window.
// --------------------------------------------------------------------------- //
const SYS_WINDOWS = [["15m", 900], ["1h", 3600], ["6h", 21600]];
const cpuColor = () => cssVar("--accent", "#4f46e5");
const memColor = () => "hsl(168 58% 42%)";
const gpuColor = (i) => `hsl(${(i * 47 + 12) % 360} 62% 52%)`;
const inColor = "hsl(211 72% 52%)";    // read / rx (data coming in)
const outColor = "hsl(28 80% 54%)";    // write / tx (data going out)

function lastNonNull(samples, sel) {
  for (let i = samples.length - 1; i >= 0; i--) { const v = sel(samples[i]); if (v != null) return v; }
  return null;
}
// Every distinct key across all samples' `list(s)` entries (devices/interfaces/GPUs come and go).
function unionKeys(samples, list, key) {
  const seen = new Set();
  for (const s of samples) for (const it of (list(s) || [])) { const k = key(it); if (k != null) seen.add(k); }
  return [...seen];
}
const memPct = (used, total) => (used != null && total) ? (100 * used) / total : null;

// Two colored current-rate chips (read+write or rx+tx) — doubles as the panel's legend.
function dualRate(aColor, aVal, bColor, bVal) {
  const chip = (color, val) => h("span", { class: "rate" },
    h("span", { class: "rate-dot", style: `background:${color}` }), fmtRate(val));
  return [chip(aColor, aVal), chip(bColor, bVal)];
}

// A device/interface card: two rate lines on an auto-scaled (bytes/s) axis + a dual-rate readout.
function RatePanel(title, sub) {
  const valEl = h("div", { class: "panel-val rates" });
  const titleEl = h("div", { class: "panel-title" }, title);
  if (sub) titleEl.title = sub;
  const chart = TimeChart({ height: 158, ymax: null, fmtY: fmtRate });
  const head = h("div", { class: "panel-head" }, titleEl, h("span", { class: "spacer" }), valEl);
  return { el: h("div", { class: "chart-panel" }, head, chart.el), chart, valEl, titleEl, destroy: () => chart.destroy() };
}

// Reconcile a grid of keyed panels to `keys`: create new, update each, drop the gone, toggle empty.
function syncPanels(grid, panels, keys, make, update, emptyEl) {
  const seen = new Set();
  for (const k of keys) {
    let p = panels.get(k);
    if (!p) { p = make(k); panels.set(k, p); }
    update(p, k);
    grid.append(p.el);                            // (re)order in key order; moves existing nodes
    seen.add(k);
  }
  for (const [k, p] of panels) if (!seen.has(k)) { p.el.remove(); p.destroy(); panels.delete(k); }
  if (!keys.length) { if (!emptyEl.isConnected) grid.append(emptyEl); }
  else if (emptyEl.isConnected) emptyEl.remove();
}

function System(root, nodeArg) {
  setNav("system"); setCrumbs([{ label: "system" }]);
  let windowSec = 3600, gpuMode = "util";        // gpuMode: "util" | "mem"
  let node = nodeArg ? decodeURIComponent(nodeArg) : null;
  let nodes = [], samples = [], lastT = null, meta = {};
  let gen = 0, dead = false;

  const sub = h("span", { class: "sub" });
  const nodeRow = h("div", { class: "node-row" });
  const winSeg = h("div", { class: "seg" });

  const redraw = (o) => o && renderCharts();    // redraw a section's charts when it's revealed

  // GPU section — one chart per GPU; the compute/memory switch in its header applies to them all.
  const gpuSeg = h("div", { class: "seg sm" });
  const gpuGrid = h("div", { class: "chart-grid" });
  const gpuEmpty = h("div", { class: "empty small" }, "No GPUs on this node.");
  const gpuSection = CollapsibleSection("GPU", { open: true, onToggle: redraw });
  gpuSection.ctrls.append(gpuSeg);
  gpuSection.body.append(gpuGrid);
  const gpuPanels = new Map();            // gpu index -> GpuPanel

  // CPU + Memory sections — one chart each, independently show/hide-able.
  const cpuChart = TimeChart({ height: 188, ymax: 100 });
  const cpuSection = CollapsibleSection("CPU usage", { open: true, onToggle: redraw });
  cpuSection.body.append(chartCard(cpuChart));
  const memChart = TimeChart({ height: 188, ymax: 100 });
  const memSection = CollapsibleSection("Memory usage", { open: true, onToggle: redraw });
  memSection.body.append(chartCard(memChart));

  // Disk + Network sections — one chart per mounted device / per interface (read+write, rx+tx).
  const diskGrid = h("div", { class: "chart-grid" });
  const diskEmpty = h("div", { class: "empty small" }, "No disk activity yet.");
  const diskSection = CollapsibleSection("Disk I/O", { open: true, onToggle: redraw });
  diskSection.body.append(diskGrid);
  const diskPanels = new Map();           // device -> RatePanel
  const netGrid = h("div", { class: "chart-grid" });
  const netEmpty = h("div", { class: "empty small" }, "No network activity yet.");
  const netSection = CollapsibleSection("Network I/O", { open: true, onToggle: redraw });
  netSection.body.append(netGrid);
  const netPanels = new Map();            // iface -> RatePanel

  root.append(
    h("div", { class: "page-head" }, h("h1", {}, "System"), sub),
    nodeRow,
    h("div", { class: "sys-controls" }, h("span", { class: "ctrl-label" }, "Window"), winSeg),
    gpuSection.el, cpuSection.el, memSection.el, diskSection.el, netSection.el);

  function drawSeg(host, items, current, onpick) {
    host.replaceChildren(...items.map(([lab, v]) =>
      h("button", { class: "seg-btn" + (v === current ? " on" : ""), onclick: () => onpick(v) }, lab)));
  }
  const drawWinSeg = () => drawSeg(winSeg, SYS_WINDOWS, windowSec,
    (sec) => { if (windowSec !== sec) { windowSec = sec; drawWinSeg(); reset(); } });
  const drawGpuSeg = () => drawSeg(gpuSeg, [["Compute", "util"], ["Memory", "mem"]], gpuMode,
    (m) => { if (gpuMode !== m) { gpuMode = m; drawGpuSeg(); renderCharts(); } });
  drawWinSeg(); drawGpuSeg();

  function reset() { samples = []; lastT = null; gen++; renderCharts(); loop.kick(); }
  function selectNode(id) { if (id !== node) { node = id; renderNodes(); reset(); } }

  function nodeSub(n) {
    const bits = [`${n.ngpu} GPU`, `${n.ncpu} CPU`];
    if (n.mem_total_mb) bits.push(`${gb(n.mem_total_mb)} GB`);
    if (n.live_runs) bits.push(`${n.live_runs} live run${n.live_runs === 1 ? "" : "s"}`);
    return bits.join(" · ");
  }
  function renderNodes() {
    if (!nodes.length) { nodeRow.replaceChildren(h("div", { class: "empty small" }, "No nodes seen yet.")); return; }
    nodeRow.replaceChildren(...nodes.map((n) =>
      h("button", { class: "node-chip" + (n.id === node ? " on" : ""), onclick: () => selectNode(n.id) },
        h("span", { class: "node-dot" + (n.live_runs ? " live" : "") }),
        h("div", { class: "node-info" },
          h("div", { class: "node-name" }, n.label),
          h("div", { class: "node-sub" }, nodeSub(n))))));
  }

  function renderCharts() {
    const now = meta.now || serverNow();
    const range = [now - windowSec, now];
    const times = samples.map((s) => s.t);

    // CPU
    cpuChart.setData({ times, range, series: [{ name: "CPU", color: cpuColor(), values: samples.map((s) => s.cpu) }] });
    const c = lastNonNull(samples, (s) => s.cpu);
    cpuSection.valEl.textContent = c == null ? "—" : Math.round(c) + "%";

    // Memory (plotted as %, hover shows GB)
    memChart.setData({ times, range, series: [{ name: "Memory", color: memColor(),
      values: samples.map((s) => memPct(s.mem_used, s.mem_total)),
      labels: samples.map((s) => memPct(s.mem_used, s.mem_total) == null ? "—" : `${gb(s.mem_used)} / ${gb(s.mem_total)} GB`) }] });
    const lm = samples.length ? samples[samples.length - 1] : null;
    memSection.valEl.textContent = lm && memPct(lm.mem_used, lm.mem_total) != null ? `${gb(lm.mem_used)} / ${gb(lm.mem_total)} GB` : "—";

    // GPU — one panel per GPU, all sharing the compute/memory mode
    const gpuVal = (g) => g ? (gpuMode === "mem" ? memPct(g.mem_used, g.mem_total) : g.util) : null;
    const gpuLabel = (g) => g && memPct(g.mem_used, g.mem_total) != null ? `${gb(g.mem_used)} / ${gb(g.mem_total)} GB` : "—";
    const gpuKeys = unionKeys(samples, (s) => s.gpus, (g) => g.i).sort((a, b) => a - b);
    const gpuCur = [];
    syncPanels(gpuGrid, gpuPanels, gpuKeys, (i) => GpuPanel(i), (p, i) => {
      const at = (s) => (s.gpus || []).find((g) => g.i === i);
      p.chart.setData({ times, range, series: [{ name: "GPU " + i, color: gpuColor(i),
        values: samples.map((s) => gpuVal(at(s))),
        labels: gpuMode === "mem" ? samples.map((s) => gpuLabel(at(s))) : null }] });
      const cur = lastNonNull(samples, (s) => gpuVal(at(s)));
      p.valEl.textContent = cur == null ? "—" : Math.round(cur) + "%";
      if (cur != null) gpuCur.push(cur);
    }, gpuEmpty);
    gpuSeg.style.display = gpuKeys.length ? "" : "none";   // no toggle when there are no GPUs
    const gpuAvg = gpuCur.length ? Math.round(gpuCur.reduce((a, b) => a + b, 0) / gpuCur.length) : null;
    gpuSection.valEl.textContent = !gpuKeys.length ? "no GPUs"
      : `${gpuKeys.length} GPU${gpuKeys.length === 1 ? "" : "s"}` + (gpuAvg != null ? ` · ${gpuAvg}% avg` : "");

    // Disk — one panel per mounted device (read + write), labeled by mountpoint
    const diskKeys = unionKeys(samples, (s) => s.disk, (d) => d.dev).sort();
    let totRead = 0, totWrite = 0;
    syncPanels(diskGrid, diskPanels, diskKeys, (dev) => RatePanel(dev, dev), (p, dev) => {
      const at = (s) => (s.disk || []).find((d) => d.dev === dev);
      const last = lastNonNull(samples, at);
      p.titleEl.textContent = (last && last.mount) || dev;
      p.titleEl.title = dev;
      p.chart.setData({ times, range, series: [
        { name: "read", color: inColor, values: samples.map((s) => { const d = at(s); return d ? d.read_bps : null; }) },
        { name: "write", color: outColor, values: samples.map((s) => { const d = at(s); return d ? d.write_bps : null; }) }] });
      p.valEl.replaceChildren(...dualRate(inColor, last && last.read_bps, outColor, last && last.write_bps));
      if (last) { totRead += last.read_bps || 0; totWrite += last.write_bps || 0; }
    }, diskEmpty);
    diskSection.valEl.textContent = diskKeys.length ? `${fmtRate(totRead)} read · ${fmtRate(totWrite)} write` : "idle";

    // Network — one panel per interface (rx + tx)
    const netKeys = unionKeys(samples, (s) => s.net, (n) => n.iface).sort();
    let totRx = 0, totTx = 0;
    syncPanels(netGrid, netPanels, netKeys, (iface) => RatePanel(iface), (p, iface) => {
      const at = (s) => (s.net || []).find((n) => n.iface === iface);
      const last = lastNonNull(samples, at);
      p.chart.setData({ times, range, series: [
        { name: "rx", color: inColor, values: samples.map((s) => { const n = at(s); return n ? n.rx_bps : null; }) },
        { name: "tx", color: outColor, values: samples.map((s) => { const n = at(s); return n ? n.tx_bps : null; }) }] });
      p.valEl.replaceChildren(...dualRate(inColor, last && last.rx_bps, outColor, last && last.tx_bps));
      if (last) { totRx += last.rx_bps || 0; totTx += last.tx_bps || 0; }
    }, netEmpty);
    netSection.valEl.textContent = netKeys.length ? `${fmtRate(totRx)} rx · ${fmtRate(totTx)} tx` : "idle";
  }

  function describeNode() {
    const n = nodes.find((x) => x.id === node);
    return !n ? (node || "") : node + (n.live_runs ? ` · ${n.live_runs} live run${n.live_runs === 1 ? "" : "s"}` : "");
  }

  const loop = adaptiveLoop(async () => {
    if (dead) return;
    const myGen = gen;
    try {
      const nd = await api("/api/system/nodes");
      if (dead || myGen !== gen) return;
      nodes = nd.nodes || [];
      if (!node) node = nd.current || (nodes[0] && nodes[0].id) || null;
      renderNodes();
    } catch { /* keep the last node list */ }
    if (!node || dead || myGen !== gen) return;
    const q = "window=" + windowSec + (lastT != null ? "&since_ts=" + lastT : "");
    const d = await api(`/api/system/${encodeURIComponent(node)}?${q}`);
    if (dead || myGen !== gen || !d) return;
    meta = d;
    samples = lastT == null ? (d.samples || []).slice() : samples.concat(d.samples || []);
    const floor = (d.now || serverNow()) - windowSec;
    samples = samples.filter((s) => s.t >= floor);
    if (samples.length) lastT = samples[samples.length - 1].t;
    sub.textContent = describeNode();
    renderCharts();
  });

  return {
    destroy() {
      dead = true; loop.cancel(); cpuChart.destroy(); memChart.destroy();
      for (const m of [gpuPanels, diskPanels, netPanels]) for (const [, p] of m) p.destroy();
    },
  };
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
  } else if (parts[0] === "system") {
    active = System(main, parts[1]);
  } else if (parts[0] === "all") {
    active = Overview(main);
  } else if (parts[0] === "group" && parts[1]) {
    active = Dashboard(main, decodeURIComponent(parts[1]));   // runs view, focused on one group
  } else {
    active = Dashboard(main);
  }
}

function initHeader() {
  const conn = document.getElementById("conn");
  const footDot = document.getElementById("foot-dot");
  const footText = document.getElementById("foot-text");
  Runs.subscribe((data, ok) => {
    const cs = connState();
    const offline = cs !== "online";
    const runs = data.runs || [];
    const live = runs.filter((r) => r.live).length;
    conn.classList.toggle("active", cs === "online" && live > 0);   // never look "active" off stale data
    conn.classList.toggle("disconnected", cs === "offline");
    conn.classList.toggle("forbidden", cs === "forbidden");
    conn.onclick = cs === "forbidden" ? manualReconnect : null;     // a 403 only clears on a full reload
    conn.title = cs === "forbidden" ? "Reload to re-authenticate the forwarded connection" : "Active runs";
    conn.replaceChildren(h("span", { class: "pulse" }),
      h("span", {}, cs === "forbidden" ? "blocked (403) — reload"
                  : cs === "offline" ? "disconnected"
                  : live ? `${live} active` : "idle"));
    if (footDot) { footDot.classList.toggle("live", cs === "online" && live > 0); footDot.classList.toggle("offline", offline); }
    if (footText) footText.textContent = cs === "forbidden" ? "blocked (403)"
      : cs === "offline" ? "disconnected"
      : runs.length ? `${runs.length} run${runs.length === 1 ? "" : "s"}` : "watching runs";
  });
}

// Sidebar "Runs" — a collapsible tree of run groups (project names) with each group's status.
// Fed by the shared /api/runs poll, so it mirrors the Runs page and updates (incl. disconnected)
// on every tick. Clicking a group opens the Runs view focused on it; the caret folds the tree.
function initRunsNav() {
  const subEl = document.getElementById("runs-sub");
  const caret = document.getElementById("runs-caret");
  if (!subEl || !caret) return;
  let open = true;
  const applyOpen = () => { caret.textContent = open ? "▾" : "▸"; subEl.hidden = !open; };
  const toggle = (e) => { e.preventDefault(); e.stopPropagation(); open = !open; applyOpen(); };
  caret.addEventListener("click", toggle);
  caret.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") toggle(e); });
  applyOpen();

  function paint() {
    const groups = groupRuns(Runs.data.runs || []);
    const m = location.hash.match(/^#\/group\/(.+)$/);
    const activeName = m ? decodeURIComponent(m[1]) : null;
    if (!groups.length) { subEl.replaceChildren(h("div", { class: "nav-sub-empty" }, "No runs yet")); return; }
    subEl.replaceChildren(...groups.map((g) => {
      const info = groupStatusInfo(g.runs);
      return h("a", {
        class: "nav-sub-item" + (g.name === activeName ? " on" : ""),
        href: `#/group/${encodeURIComponent(g.name)}`,
        title: `${g.name} — ${info.label}`,
      },
        h("span", { class: "dot" + (info.live ? " live" : ""), style: `background:${stateColor(info.tone)}` }),
        h("span", { class: "nav-sub-name" }, g.name),
        h("span", { class: "nav-sub-status" }, info.label));
    }));
  }
  Runs.subscribe(() => paint());
  window.addEventListener("hashchange", paint);
}

window.addEventListener("hashchange", route);
for (const ev of ["pointerdown", "pointermove", "keydown", "wheel", "scroll", "touchstart"])
  window.addEventListener(ev, markActivity, { passive: true });
window.addEventListener("focus", markActivity);
document.addEventListener("visibilitychange", () => { if (!document.hidden) markActivity(); });
initHeader();
initRunsNav();
Runs.start();
route();
