"use strict";
// pipelines dashboard — a small single-page app. No framework, no build step.
//
//   • a shared poll of /api/runs drives the header and the dashboard grid
//   • run detail and log views stream live over Server-Sent Events
//   • a hash router (#/ , #/run/<port> , #/run/<port>/log/<slug>) swaps views
//
// Views are functions that mount into <main> and return { destroy } so the router can
// tear down timers and event sources cleanly.

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

const STATES = ["running", "yielding", "queued", "held", "blocked", "completed", "failed", "cancelled"];
const RANK = Object.fromEntries(STATES.map((s, i) => [s, i]));
const TERMINAL = new Set(["completed", "failed", "cancelled", "blocked"]);

const stateColor = (s) => `var(--${STATES.includes(s) ? s : "queued"})`;
const dot = (s) => h("span", { class: "dot", style: `background:${stateColor(s)}` });

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
  if (!ts) return "";
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
  return h("span", { class: "pill" }, dot(state), state);
}

function segBar(counts, total) {
  const bar = h("div", { class: "bar" });
  if (!total) return bar;
  for (const s of STATES) {
    const n = counts[s] || 0;
    if (n) bar.append(h("span", { style: `width:${(100 * n) / total}%;background:${stateColor(s)}`, title: `${s}: ${n}` }));
  }
  return bar;
}

function tallies(counts) {
  const wrap = h("div", { class: "tallies" });
  for (const s of STATES) {
    const n = counts[s] || 0;
    if (n) wrap.append(h("span", { class: "tally" }, dot(s), h("b", {}, n), s));
  }
  return wrap;
}

function poolShort(pool) {
  const bits = [];
  const g = pool && pool.gpus, c = pool && pool.cpus;
  if (g && g.total) bits.push(`${g.total - g.free}/${g.total} gpu`);
  if (c && c.total) bits.push(`${c.total - c.free}/${c.total} cpu`);
  return bits.join(" · ");
}

function setCrumbs(items) {
  const c = document.getElementById("crumbs");
  c.replaceChildren();
  items.forEach((it, i) => {
    if (i) c.append(h("span", { class: "sep" }, "›"));
    c.append(it.href ? h("a", { href: it.href }, it.label) : h("span", { class: "here" }, it.label));
  });
}

// --------------------------------------------------------------------------- //
// Dashboard (#/)
// --------------------------------------------------------------------------- //
function Dashboard(root) {
  setCrumbs([]);
  const sub = h("span", { class: "sub" });
  const cards = h("div", { class: "cards" });
  root.append(h("div", { class: "page-head" }, h("h1", {}, "Runs"), sub), cards);

  let sig = null;
  const unsub = Runs.subscribe((data) => {
    const runs = data.runs || [];
    sub.textContent = runs.length ? `${runs.length} run${runs.length > 1 ? "s" : ""}` : "";
    const next = JSON.stringify(runs.map((r) => [r.port, r.status, Math.round(r.elapsed || 0), r.counts, poolShort(r.pool)]));
    if (next === sig) return;                       // nothing visible changed; skip the churn
    sig = next;
    cards.replaceChildren(runs.length ? null : emptyState(), ...runs.map(runCard));
  });
  return { destroy: unsub };
}

function runCard(r) {
  return h("a", { class: "card", href: `#/run/${r.port}` },
    h("div", { class: "card-head" },
      h("span", { class: "card-title" }, r.project || "run"),
      h("span", { class: "card-port" }, `:${r.port}`),
      h("span", { style: "margin-left:auto" }, pill(r.status))),
    h("div", { class: "card-sub" }, cardSub(r)),
    segBar(r.counts, r.n_jobs),
    tallies(r.counts));
}

function cardSub(r) {
  const bits = [];
  bits.push((r.live ? "running " : (r.status === "completed" || r.status === "failed" ? r.status + " " : "")) + fmtDur(r.elapsed));
  if (r.started_at) bits.push(fmtAgo(r.started_at));
  const p = poolShort(r.pool);
  if (p) bits.push(p);
  return bits.join("  ·  ");
}

function emptyState() {
  return h("div", { class: "empty" },
    h("div", { class: "big" }, "No runs yet"),
    h("div", {}, "Start one with ", h("code", {}, "pipelines runparallel"), " and it will show up here."));
}

// --------------------------------------------------------------------------- //
// Run detail (#/run/<port>) — SSE snapshot + live event records
// --------------------------------------------------------------------------- //
function RunDetail(root, port) {
  setCrumbs([{ label: `run :${port}` }]);
  const head = h("div", { class: "page-head" });
  const meta = h("div", { class: "meta-row" });
  const gauges = h("div", { class: "gauges" });
  const filters = h("div", { class: "filters" });
  const jobs = h("div", { class: "jobs" });
  root.append(head, meta, gauges, filters, jobs);

  const M = { jobs: new Map(), order: [], names: {}, pool: {}, status: "", live: false,
              started_at: null, ended_at: null, project: null, store: null, logdir: null };
  const hidden = new Set();
  const rows = new Map();
  let raf = 0, dead = false;

  function applySnapshot(d) {
    Object.assign(M, {
      status: d.status, live: d.live, started_at: d.started_at, ended_at: d.ended_at,
      project: d.project, store: d.store, logdir: d.logdir, pool: d.pool || {},
    });
    M.jobs.clear(); M.order = []; M.names = {};
    for (const j of d.jobs || []) { M.jobs.set(j.relpath, j); M.order.push(j.relpath); M.names[j.relpath] = j.name; }
    setCrumbs([{ label: (d.project ? d.project + " " : "") + `:${port}` }]);
  }

  function applyRecord(rec) {
    if (rec.type === "job_state" && rec.relpath) {
      const rp = rec.relpath, prev = M.jobs.get(rp) || {};
      const job = { ...prev, ...rec };
      job.name = M.names[rp] || prev.name || rec.cls || rp.split("/")[0];
      job.slug = prev.slug || slugify(rp);
      M.names[rp] = job.name;
      if (!M.jobs.has(rp)) M.order.push(rp);
      M.jobs.set(rp, job);
    } else if (rec.type === "pool") {
      M.pool = { gpus: rec.gpus, cpus: rec.cpus, memory_mb: rec.memory_mb };
    } else if (rec.type === "server_done") {
      M.live = false;
      M.status = rec.ok ? "completed" : "failed";
      M.ended_at = M.ended_at || serverNow();
    }
  }

  function counts() {
    const c = {};
    for (const rp of M.order) { const st = (M.jobs.get(rp) || {}).state || "queued"; c[st] = (c[st] || 0) + 1; }
    return c;
  }

  const scheduleRender = () => { if (!raf) raf = requestAnimationFrame(() => { raf = 0; render(); }); };

  function render() {
    if (dead) return;
    const elapsed = (M.ended_at != null ? M.ended_at : serverNow()) - (M.started_at || serverNow());
    head.replaceChildren(
      h("h1", {}, M.project || "run"),
      pill(M.status),
      h("span", { class: "sub" }, `${M.live ? "running" : M.status} · ${fmtDur(elapsed)}`));

    const m = [kv("jobs", M.order.length)];
    if (M.started_at) m.push(kv("started", fmtAgo(M.started_at)));
    if (M.store) m.push(kv("store", M.store, true));
    if (M.logdir) m.push(kv("logs", M.logdir, true));
    meta.replaceChildren(...m);

    gauges.replaceChildren(...gaugeEls(M.pool));
    renderFilters();
    renderJobs();
  }

  function renderFilters() {
    const c = counts();
    filters.replaceChildren(...STATES.filter((s) => c[s]).map((s) =>
      h("span", { class: "chip" + (hidden.has(s) ? " off" : ""),
        onclick: () => { hidden.has(s) ? hidden.delete(s) : hidden.add(s); render(); } },
        dot(s), s, h("b", {}, c[s]))));
  }

  function renderJobs() {
    const list = M.order.map((rp) => M.jobs.get(rp)).filter(Boolean)
      .filter((j) => !hidden.has(j.state))
      .sort((a, b) => (RANK[a.state] ?? 9) - (RANK[b.state] ?? 9) || (a.name || "").localeCompare(b.name || ""));
    if (!list.length) { jobs.replaceChildren(h("div", { class: "empty" }, hidden.size ? "No jobs match the filter." : "No jobs.")); rows.clear(); return; }
    if (jobs.firstChild && jobs.firstChild.classList && jobs.firstChild.classList.contains("empty")) jobs.firstChild.remove();
    const seen = new Set();
    for (const j of list) {                          // keyed update: keep nodes, just reorder/patch
      let row = rows.get(j.relpath);
      if (!row) { row = makeRow(); rows.set(j.relpath, row); }
      patchRow(row, j);
      jobs.append(row);
      seen.add(j.relpath);
    }
    for (const [rp, row] of rows) if (!seen.has(rp)) { row.remove(); rows.delete(rp); }
  }

  function makeRow() {
    return h("a", { class: "job" },
      dot("queued"),
      h("div", { style: "min-width:0" }, h("div", { class: "name" }), h("div", { class: "relpath" })),
      h("div", { class: "state" }),
      h("div", { class: "gpus" }),
      h("div", { class: "when" }));
  }

  function patchRow(row, j) {
    row.href = `#/run/${port}/log/${encodeURIComponent(j.slug || slugify(j.relpath))}`;
    row.className = `job s-${j.state}`;
    row.children[0].style.background = stateColor(j.state);
    row.children[1].children[0].textContent = j.name || j.relpath;
    row.children[1].children[1].textContent = j.relpath;
    row.children[2].textContent = j.state + (j.held ? " · held" : "");
    row.children[3].textContent = gpuText(j);
    row.children[4].textContent = elapsedText(j);
    row.title = j.reason || "";
  }

  const es = new EventSource(`/api/runs/${port}/stream`);
  es.addEventListener("snapshot", (e) => { applySnapshot(JSON.parse(e.data)); render(); });
  es.addEventListener("end", () => { es.close(); render(); });
  es.onmessage = (e) => { applyRecord(JSON.parse(e.data)); scheduleRender(); };

  const tick = setInterval(() => { if (M.live) scheduleRender(); }, 1000);  // keep elapsed live

  return { destroy() { dead = true; es.close(); clearInterval(tick); } };
}

function kv(k, v, mono) {
  return h("div", {}, h("span", { class: "k" }, k), h("span", { class: "v" + (mono ? " mono" : "") }, v));
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
      const j = (d.jobs || []).find((x) => x.slug === slug || slugify(x.relpath) === slug);
      if (j) {
        title.textContent = j.name || slug;
        statusEl.replaceChildren(statePill(j.state));
        metaEl.textContent = elapsedText(j);
        setCrumbs([{ label: (d.project ? d.project + " " : "") + `:${port}`, href: `#/run/${port}` }, { label: j.name || slug }]);
      }
      if (d.logdir) pathEl.textContent = `${d.logdir}/jobs/${slug}.log`;
      const running = d.status === "live" && (!j || !TERMINAL.has(j.state));
      if (running) infoTimer = setTimeout(info, 2000);
    } catch {
      if (!dead) infoTimer = setTimeout(info, 4000);
    }
  }
  info();

  return { destroy() { dead = true; es.close(); clearTimeout(infoTimer); } };
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
  } else {
    active = Dashboard(main);
  }
}

function initHeader() {
  const conn = document.getElementById("conn");
  Runs.subscribe((data, ok) => {
    const live = (data.runs || []).filter((r) => r.live).length;
    conn.classList.toggle("active", live > 0);
    conn.classList.toggle("disconnected", ok === false);
    conn.replaceChildren(h("span", { class: "pulse" }),
      h("span", {}, ok === false ? "disconnected" : live ? `${live} active` : "idle"));
  });
}

window.addEventListener("hashchange", route);
initHeader();
Runs.start();
route();
