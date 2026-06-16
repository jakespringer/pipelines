# Dashboard — `pipelines/dashboard/`

**Provides:** `pipelines dashboard` — a small, dependency-free **local** web monitor for **remote** runs.
**Modules:** `pipelines/dashboard/index.py`, `pipelines/dashboard/server.py`, `pipelines/dashboard/metrics.py`, `pipelines/dashboard/assets/`.
**Depends on:** [09 CLI](09-cli.md) (verb dispatch), `pipelines/scheduler/` ([registry and event log](../pipelines/scheduler/)),
`pipelines/reporting/` ([the SSH shipper that delivers a run's files here](../pipelines/reporting/)),
`pipelines/identity.py` (`slug`, `validate_under_base`).
**Status:** implemented; standard-library only (no third-party packages, no build step).

The guiding rule mirrors the CLI's: **one source of truth.** The dashboard never opens a run server's
control socket. It reads the same artifacts a run already writes — the registry entry and the
append-only `events.log` — so a *live* run and a *finished* one are served by identical code, and the
monitor keeps working after a run (or the dashboard itself) has exited.

The dashboard runs on a **local** machine and is a pure **viewer**. Jobs run anywhere and **ship**
their files to it over SSH (see [`pipelines/reporting/`](../pipelines/reporting/) and §1b). SSH — not
an HTTP port — is the only ingress for run data, so the server stays bound to localhost for the
browser. Reporting is **off by default**: a run ships only when its project configures
`[config.dashboard]`.

---

## 1. What it reads

Everything lives under the dashboard machine's registry directory
(`$XDG_CACHE_HOME/pipelines/runs/`, see `scheduler/registry.py`). For a **remote** run the reporting
Shipper writes, over SSH:

- `<run_id>.json` — the registry entry (`kind: "remote"`), carrying the run's identity
  (`project`/`store`/`base_path`/`node`/`started_at`) and a `heartbeat_stale` window. It is **never
  removed**, so finished runs stay browsable.
- `<run_id>/` — the run's log directory, `rsync`'d from the job machine: `events.log` (one
  self-describing JSON record per line) and `jobs/<slug>.log` (each job's combined stdout/stderr).
- `<run_id>/heartbeat` — `touch`ed each Shipper tick (liveness), and `<run_id>/done` — written with
  `{ok, counts}` at completion.

`events.log` is the record of truth. Replaying it reconstructs the whole run; tailing it yields live
updates. The event types are documented in `scheduler/events.py`; `server_start` carries
`project`/`store`/`base_path` so a replayed log is fully self-describing for a finished run.

---

## 1a. Run kinds and liveness

A run is keyed by an opaque **`run_id`** string. `registry.alive` dispatches liveness on the entry's
`kind`:

- **`remote`** — a run reporting over SSH (the path the dashboard shows). Liveness is the `heartbeat`
  file's mtime (within `heartbeat_stale`) and the absence of `done` — both files on *this* machine, so
  liveness is immune to clock skew with the job machine. `logdir` is omitted from the shipped entry
  (the job can't know this machine's home) and **derived** as `<registry_dir>/<run_id>` by
  `registry.logdir_of`.
- **`slurm`** — a `SlurmExecutor` run; same heartbeat/`done` scheme, but on the run's own (possibly
  shared) filesystem. Its monitor (`scheduler/slurm_monitor.py`) *also* ships over SSH when
  `[config.dashboard]` is set, so the same run is browsable both on a FS-sharing dashboard and a
  remote one.
- **`parallel`** — retained for `pipelines attach` (TCP port probe). The dashboard no longer discovers
  these directly; a `runparallel` job appears on the dashboard only by reporting over SSH like any
  other run.

The HTTP routes and the whole JS frontend treat the id as an opaque string, so these kinds are
invisible to them.

---

## 1b. Shipping model — how a run reaches the dashboard

A run's events are produced by the scheduler (`RunServer`) or the Slurm monitor and written to a local
`events.log` via a `Reporter` ([`pipelines/reporting/reporter.py`](../pipelines/reporting/reporter.py)),
a drop-in for the scheduler's `EventLog`. When `[config.dashboard]` is configured, the `Reporter`
also starts a background `Shipper` thread that mirrors the local files to the dashboard machine. The
local `events.log` is the durable source of truth and the buffer: `emit()` only appends to it and never
touches the network, so an unreachable dashboard can never slow a job.

Each Shipper tick (≈ `interval` s), over one multiplexed SSH connection (`ControlMaster`):

- `rsync --append --inplace` the `events.log` and the `jobs/` logs — **append-only**, so `--append`
  ships only the byte delta and is correct across interrupts (the remote file is always a true prefix
  of the local one). A reconnect after any outage therefore replays exactly what the dashboard
  missed — no acknowledgements, no custom replay code.
- `touch` the remote `heartbeat`.
- if this process owns the host metrics lock, `rsync` (plain, **not** `--append`) the `<node>.jsonl`
  (it is a rewritten, trimmed ring, not a growing prefix — see §5).

On failure the Shipper backs off exponentially up to `backoff_max` (default 300 s) and retries; every
ssh/rsync call is bounded by a timeout and wrapped, so nothing ever propagates into the job. The
registry entry is written once by piping JSON over plain `ssh` (`cat >`), so the dashboard machine
needs only `sshd` + `rsync` + coreutils to ingest — no pipelines install, no version coupling.

**Configuration** is the `[config.dashboard]` table, read via `Project.config` (off unless `host`
set):

| key | default | meaning |
|-----|---------|---------|
| `host` | — | dashboard SSH host (`user@host` or an ssh-config alias); presence enables reporting |
| `enabled` | `true` | explicit master switch (`false` forces off even with `host` set) |
| `port` / `user` / `identity` | `22` / — / — | SSH connection details (`identity` ⇒ `-i`; omit for ssh-agent) |
| `runs_dir` | `~/.cache/pipelines/runs` | the **remote** registry dir (rsync/cat destination) |
| `metrics` | `false` | also sample + ship this host's system metrics (one producer per host) |
| `interval` | `5.0` | Shipper tick (s); drives `heartbeat_stale = max(45, interval*3)` |
| `backoff_max` | `300.0` | cap on the exponential retry interval (s) |
| `node` | host short name | override the reported node id |

Put switches (`enabled`/`metrics`) in the versioned `pipelines.toml`; put host/identity in the
unversioned per-project overlay `~/.config/pipelines/projects/<name>.toml`, so secrets stay out of the
repo (see [10 config & packaging](10-config-project-packaging.md)).

```toml
# pipelines.toml (versioned)
[config.dashboard]
enabled = true
metrics = true

# ~/.config/pipelines/projects/<name>.toml (machine-specific)
[config.dashboard]
host = "me@dash.local"
identity = "~/.ssh/id_pipelines"
```

**`run_id` uniqueness.** Many hosts report to one dashboard, so the id must be globally unique:
`RunServer` mints `<short-host>-<unix_ts>-<pid>`; slurm reuses its `slurm-<fingerprint>`.

**Accumulation.** `remote`/`slurm` entries are never auto-pruned (like a finished slurm run, their json
is the only browsable handle). They accumulate under the registry dir; prune old, `done`-marked runs
manually as needed.

---

## 2. `index.py` — discovery and replay

`RunView` replays one run's `events.log` into queryable state, **incrementally**: each `refresh()`
reads only the bytes appended since the last call (tracking a byte offset and buffering a trailing
partial line) and returns the new records. So a streaming endpoint can forward those records verbatim,
and a periodic poll is cheap. State is just: jobs (latest snapshot per relpath, seeded as `queued`
from `server_start`), declaration order, the resource pool, start/end, and the project identity.
Display labels (`Class#i`) and per-state counts are derived on read — the same scheme the `attach`
TUI uses, so the two surfaces read alike.

`RunIndex` discovers every run from a **single** source — each `<run_id>.json` registry entry, whose
`logdir` (via `registry.logdir_of`) names the dir holding `events.log` — caches a `RunView` per
`run_id`, and renders the JSON payloads. A run whose `events.log` hasn't landed yet (entry written,
first rsync pending) is skipped until it does. Liveness reuses `registry.list_runs`, which only checks
runs that own a registry file (heartbeat for slurm/remote) — so the **history is never probed**, only
the few candidates that could be alive.

---

## 3. `server.py` — HTTP + SSE

A stdlib `ThreadingHTTPServer` serves the single-page UI and a small API:

| Route | Result |
|-------|--------|
| `GET /` (+ `/app.js`, `/style.css`, `/favicon.svg`) | the UI assets |
| `GET /api/runs` | JSON list of every run + summary |
| `GET /api/overview` | JSON every run's full detail — backs the expanded "all runs" tab |
| `GET /api/runs/<port>` | JSON detail of one run, grouped into pipeline **steps** |
| `GET /api/runs/<port>/stream` | **SSE**: a `snapshot`, then live event records, then `end` |
| `GET /api/runs/<port>/log/<slug>` | a job's full log as `text/plain` (`?download` to save) |
| `GET /api/runs/<port>/log/<slug>/stream` | **SSE**: the log so far, then live-tailed appends |
| `GET /api/system/nodes` | JSON the nodes we sample + each one's latest reading |
| `GET /api/system/<node>` | JSON a node's GPU/CPU/memory/disk/network time-series (`?window` seconds, `?since_ts` for deltas) |

**Update model.** The lists (runs, all-runs) and the run-detail view **poll** their JSON endpoints and
re-render via keyed diffs — simple and robust: a missed update, dropped connection, or re-run is always
reconciled on the next tick. The poll interval is **adaptive**: ~2 s while the window is being used,
easing linearly to 60 s after ~10 min of inactivity (tracked from pointer/key/scroll/focus and tab
visibility); returning from idle kicks the live loops to refresh immediately. **Disconnection** is
just a failed poll: the shared `/api/runs` heartbeat is the single "is the server reachable" signal —
when it fails the topbar badge and the run-detail status read **disconnected** (rather than a stale,
still-pulsing *live*), and any failing loop drops the idle ramp to retry every ~3 s, so a server that
restarts (or comes back on a new machine) is picked up within seconds and every view re-queries from
scratch — the page holds no run state of its own. A **403** is treated specially: it's a reachable
server *refusing* us (typically a VS Code / dev-tunnel port-forward whose auth cookie lapsed when the
server bounced), which `fetch`/SSE can't re-handshake — only a full navigation can. So a persistent
403 triggers a guarded `location.reload()` (at most twice per tab session, ≥10 s apart) to re-auth,
falling back to a clickable **blocked (403) — reload** badge if that doesn't take, so a genuinely
forbidden port can't trap the tab in a reload loop. The run-detail page also
runs a 1 s client tick (while visible) so the elapsed clock stays smooth between polls — and the
client→server clock offset is re-synced **at most every 30 s**, so the clock advances off the local
`Date.now()` and ticks without jittering on each poll. **Logs** stream over SSE, since tailing
append-only output is what SSE is for; each SSE payload is a JSON value so log text with arbitrary
newlines never collides with SSE's line framing. The run-detail SSE stream endpoint still exists
(snapshot + forwarded `events.log` records) but the UI prefers polling for reliability. SSE streams
notice a client disconnect on their next write; daemon threads mean nothing blocks shutdown. The log
view **owns its reconnection** rather than relying on `EventSource`'s transparent retry: because the
server replays each log from the top on a fresh connection, the client closes, clears its buffer, and
re-opens on the same ~3 s cadence after an error — so a server restart never duplicates the tail.

`dashboard` is wired in `cli.py` exactly like `attach`: project-independent (handled before project
discovery), with its own `--port` (default 7000; scans upward if taken) / `--host` / `--open` flags.

---

## 4. Step grouping — mirroring `pipelines plan`

A run's detail payload is not a flat job list; it is grouped into **steps**, one per artifact type,
ordered by **depth** (upstream inputs first) — the same collapse-by-type that `pipelines plan` shows.
`_build_steps` (in `index.py`) computes each artifact's longest-path depth from its in-plan `deps`,
buckets by class, records the cross-type dependency hint (`from_types`), and trims the longest common
relpath prefix so each instance shows only what distinguishes it. Each step carries its instances with
live state; counts per state are derived on the client.

Crucially the structure includes artifacts the run **did not** execute: ones already committed
(recorded in the `server_start` `plan` with `cached: true`, shown as `cached`) and unneeded
`@artifact(transient=True)` intermediates pruned because nothing running depended on them
(`skipped: true`, shown as `skipped`). So one run's view illustrates the whole experiment: what it
built, what is building now, what was reused from a previous run, and what was deliberately skipped. A
cancelled job stays `cancelled` for that run; re-launching is simply a *new* run whose freshness pass
either finds the output now committed (it shows as `cached`) or rebuilds it — no cross-run state to
reconcile.

## 5. `metrics.py` — a producer and a reader

The System page is split into a **producer** (`SystemSampler`, on the job machine) and a **reader**
(`MetricsStore`, on the dashboard). The dashboard machine **does not sample** — it is a viewer.

`SystemSampler` runs a daemon thread on a job machine (driven by the reporting Shipper when
`[config.dashboard].metrics` is set) that samples *that host* every few seconds into a rolling,
time-bounded ring:

- **CPU** — the busy fraction over the sampling interval, from successive `/proc/stat` `cpu` lines.
- **Memory** — `MemTotal − MemAvailable` from `/proc/meminfo`.
- **GPU** — per-GPU compute utilization and memory from `nvidia-smi` (absent ⇒ no GPUs). Reuses the
  same detection philosophy as `scheduler/resources.py`, but queried live and per device.
- **Disk** — per-device read/write **throughput** (bytes/s) from `/proc/diskstats` sector counters,
  labeled by mountpoint via `/proc/mounts` (so only filesystems actually in use appear, with
  `/dev/mapper` and `by-uuid` symlinks resolved). Block devices, so it's filesystem-granular per device.
- **Network** — per-interface rx/tx throughput (bytes/s) from `/proc/net/dev` (`lo` and never-used
  interfaces dropped).

Disk and network are **rates**: like CPU, they're counter deltas over the interval — the sampler keeps
the previous reading and divides by the elapsed time. Each probe is best-effort: a failure contributes
a gap, never a crash. Each sample also carries its `node` id and `ncpu` so the reader can label and
scale a node it can't introspect. The ring is **persisted** to a local
`metrics/<node>.jsonl` (atomic rewrite every ~60 s, trimmed to the retention window); the Shipper
`rsync`s that file to the dashboard's `$XDG_CACHE_HOME/pipelines/metrics/`. Because the file is a
**rewritten** (trimmed) ring — not a growing prefix — it ships with **plain** `rsync` (atomic rename
on the destination), *never* `--append` (which is reserved for the append-only `events.log`).

**One producer per host.** Node metrics are host-wide, so co-located jobs must not each sample the same
`<node>.jsonl`. The producer takes a host-level `flock` (`metrics_producer.py`); the winner samples and
its Shipper ships, the losers ship events only. `flock` releases on process death, so a later job picks
the sampler up on its next tick.

`MetricsStore` (the dashboard reader) is purely file-based: it lists every `<node>.jsonl` in the
metrics dir (local plus every remote node rsync'd in) and serves the node-first API — `nodes()`
summarizes all of them (with per-node `live_runs`), `series(node, window, since_ts)` slices one node's
file by time, returning only samples newer than `since_ts` so a polling client transfers just the few
new points. A torn final line arriving mid-rsync is skipped, exactly like the run-event replay. The
return shapes are unchanged, so the HTTP and UI layers are untouched.

## 6. `assets/` — the UI

One HTML shell, one stylesheet, one script (vanilla JS — no framework, no build). The layout is a
standard SaaS shell: a left **sidebar** (brand + nav) and a slim topbar (breadcrumbs + a live-runs
indicator). The sidebar's **Runs** entry is itself a **collapsible tree of run groups** (one per
project) with each group's status (live / disconnected / completed / failed) fed by the shared
`/api/runs` poll; clicking a group opens the Runs view focused on it (`#/group/<name>`). A hash router
swaps the views:

- **Runs** (`#/`) — a poll of `/api/runs` rendered as four **stat cards** (total / active / completed /
  failed) above runs **grouped by name** (project): each name is a collapsible table of its runs,
  newest first, so re-runs accumulate as history instead of overwriting the previous one. A group
  auto-opens while it has a live run (and the most recent group stays open); its header shows the
  latest outcome + run count, and each row is one run (host · start time, status, progress) that links
  to its detail. Status filter + search included.
- **All runs** (`#/all`) — a sidebar view that polls `/api/overview` and renders *every* run expanded
  inline, each with the same step structure.
- **Run detail** (`#/run/<port>`) — polls the full snapshot on an interval: stat cards, pool gauges, an
  overall progress bar, and a **sortable table of pipeline steps** whose rows expand to their instances.
- **Log** (`#/run/<port>/log/<slug>`) — an SSE tail with follow-on-scroll.
- **System** (`#/system[/<node>]`) — a node selector (click a node to switch) and five **collapsible
  sections** — **GPU**, **CPU**, **Memory**, **Disk I/O**, **Network I/O** — each with a caret header
  that shows/hides it (▾ shown / ▸ hidden, the same affordance as a run's steps). GPU holds **one chart
  per GPU** with a single Compute/Memory switch in its header that retargets all of them; **Disk** and
  **Network** hold **one chart per mounted device / per interface**, each plotting two lines (read+write,
  rx+tx) on an auto-scaled bytes/s axis. Time is on the x-axis, defaulting to the past hour (15m / 1h /
  6h). Charts are drawn on a `<canvas>` by a small dependency-free `TimeChart` (gridlines, a y-axis that
  is either fixed-percent or auto-scaled with a byte-rate formatter, a hover readout — GB for memory,
  human rates for I/O). It polls `/api/system/<node>` **incrementally** with `since_ts`, appending new
  samples and dropping ones that fall out of the trailing window — so the steady-state poll is tiny.

`StepsView` is the shared component for the step list (run-detail and all-runs both feed it). It is a
continuous table — a row per artifact type, divided by hairlines — with a sortable header in run
detail (Step / Artifacts / Status / Progress; each header cycles ascending → descending → back to
pipeline order). It owns its expand state: a step auto-expands while it has active work and collapses
when idle, unless the user has clicked it — then their choice sticks. An expanded step is itself a
small sortable sub-table — **Index · Name · Status · Elapsed** — defaulting to status order (running
on top, cached at the bottom) and sortable by any column, per step. Rows are keyed (by relpath /
port / type) so live updates and re-sorts never flicker. Clicking an instance opens its log; cached instances are shown muted and are not links
(they produced no log this session). The shared `/api/runs` poll also tracks the server/client clock
offset so elapsed times survive clock skew. The theme is intentionally restrained — neutral surfaces, a
single indigo accent, status hues used only for small marks (dots, thin bars, pills), light/dark by OS
preference.

---

## 7. Extending it

- **A new read view** is a branch in `_DashboardHandler._route` plus a `RunIndex`/`RunView` method and
  a view function in `app.js`. The payload builders (`_summary`/`_detail`/`_build_steps`/`_instance`)
  are the seam to add fields to.
- **Control actions** (cancel / hold / release) are intentionally *out* of the read path, and a remote
  run has no port reachable from the dashboard — so they are unavailable for remote runs in v1.
  Routing a command back over the Shipper's SSH connection (or to a co-located `RunClient`) is the
  natural extension; keeping reads file-based is what lets the monitor work for dead runs.
- **More metrics.** The System page is already multi-node via shipped `<node>.jsonl` files. A new metric
  is one more field in `SystemSampler._sample()` (and the reader passes it through unchanged) plus one
  more section: a per-entity metric (like disk/network) is a `syncPanels` grid of `RatePanel`s; a scalar
  is a single chart in a `CollapsibleSection`. Natural next steps the same `/proc`-counter-delta pattern
  covers: NFS per-mount bytes (`/proc/self/mountstats`) and InfiniBand/RDMA counters
  (`/sys/class/infiniband/*`, which bypass `/proc/net/dev`). Per-Slurm-compute-node metrics (the monitor
  samples its login node today) would mean a sampler on each allocation feeding the same shape.
