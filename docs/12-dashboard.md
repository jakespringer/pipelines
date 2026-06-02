# Dashboard — `pipelines/dashboard/`

**Provides:** `pipelines dashboard` — a small, dependency-free web monitor for parallel runs.
**Modules:** `pipelines/dashboard/index.py`, `pipelines/dashboard/server.py`, `pipelines/dashboard/assets/`.
**Depends on:** [09 CLI](09-cli.md) (verb dispatch), `pipelines/scheduler/` ([the run server, registry, and
event log](../pipelines/scheduler/)), `pipelines/identity.py` (`slug`, `validate_under_base`).
**Status:** implemented; standard-library only (no third-party packages, no build step).

The guiding rule mirrors the CLI's: **one source of truth.** The dashboard never opens a run server's
control socket. It reads the same artifacts a run already writes — the registry entry and the
append-only `events.log` — so a *live* run and a *finished* one are served by identical code, and the
monitor keeps working after a run (or the dashboard itself) has exited.

---

## 1. What it reads

A `runparallel` run leaves two things under the registry directory
(`$XDG_CACHE_HOME/pipelines/runs/`, see `scheduler/registry.py`):

- `<port>.json` — a discovery entry written while the server is serving and removed when it exits.
  Its presence (plus a liveness probe) is how we decide a run is **live**, and it carries the run's
  identity (`project`/`store`/`base_path`).
- `<port>/` — the run's log directory: `events.log` (one self-describing JSON record per line,
  flushed eagerly) and `jobs/<slug>.log` (each job's combined stdout/stderr).

`events.log` is the record of truth. Replaying it reconstructs the whole run; tailing it yields live
updates. The event types are documented in `scheduler/events.py`; `server_start` was extended to
carry `project`/`store`/`base_path` so a replayed log is fully self-describing for a finished run.

---

## 2. `index.py` — discovery and replay

`RunView` replays one run's `events.log` into queryable state, **incrementally**: each `refresh()`
reads only the bytes appended since the last call (tracking a byte offset and buffering a trailing
partial line) and returns the new records. So a streaming endpoint can forward those records verbatim,
and a periodic poll is cheap. State is just: jobs (latest snapshot per relpath, seeded as `queued`
from `server_start`), declaration order, the resource pool, start/end, and the project identity.
Display labels (`Class#i`) and per-state counts are derived on read — the same scheme the `attach`
TUI uses, so the two surfaces read alike.

`RunIndex` discovers every run (scan the registry dir for `<port>/events.log`, plus any log dirs named
by live registry entries), caches a `RunView` per port, and renders the JSON payloads. Liveness reuses
`registry.list_runs`, which only probes ports that own a registry file — so the **history is never
TCP-probed**, only the few candidates that could be alive.

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

Live data uses Server-Sent Events. The run stream sends one snapshot then forwards each new
`events.log` record (job state, pool, done) — exactly what the run server broadcasts to a socket
client, but sourced from the file. The log stream tails `jobs/<slug>.log` from a byte offset.

Two deliberate choices keep the wire robust: **every SSE payload is a JSON value** (so log text with
arbitrary newlines never collides with SSE's line framing — the client `JSON.parse`s each `data:`),
and finished/historical runs send their snapshot (or full log) and then `end` immediately, so no
thread lingers on a run that will never change. Each stream notices a client disconnect on its next
write; daemon threads mean nothing blocks shutdown.

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

Crucially the structure includes artifacts the run **did not** execute because they were already
committed — recorded in the `server_start` `plan` with `cached: true`. So one run's view illustrates
the whole experiment: what it built, what is building now, and what was reused from a previous run. A
cancelled job stays `cancelled` for that run; re-launching is simply a *new* run whose freshness pass
either finds the output now committed (it shows as `cached`) or rebuilds it — no cross-run state to
reconcile.

## 5. `assets/` — the UI

One HTML shell, one stylesheet, one script (vanilla JS — no framework, no build). A hash router swaps
four views:

- **Runs** (`#/`) — a poll of `/api/runs` as run cards (each a small progress bar + state tallies).
- **All runs** (`#/all`) — a sibling tab that polls `/api/overview` and renders *every* run expanded
  inline, each with the same step structure.
- **Run detail** (`#/run/<port>`) — an SSE `snapshot` + live records, rendered as collapsible pipeline
  steps with pool gauges and an overall progress bar.
- **Log** (`#/run/<port>/log/<slug>`) — an SSE tail with follow-on-scroll.

`StepsView` is the shared component for the step list (run-detail and all-runs both feed it). It owns
its expand state: a step auto-expands while it has active work and collapses when idle, unless the user
has clicked it — then their choice sticks. Rows are keyed by relpath so live updates never flicker.
Clicking an instance opens its log; cached instances are shown muted and are not links (they produced
no log this session). The shared `/api/runs` poll also tracks the server/client clock offset so elapsed
times survive clock skew. The theme is intentionally restrained — neutral surfaces, a few muted status
hues used only for small marks, light/dark by OS preference.

---

## 6. Extending it

- **A new read view** is a branch in `_DashboardHandler._route` plus a `RunIndex`/`RunView` method and
  a view function in `app.js`. The payload builders (`_summary`/`_detail`/`_build_steps`/`_instance`)
  are the seam to add fields to.
- **Control actions** (cancel / hold / release) are intentionally *out* of the read path: add them as
  `POST` routes that open a `pipelines.scheduler.client.RunClient` to the run's live port and issue the
  command. Keeping reads file-based is what lets the monitor work for dead runs; control naturally
  applies only to live ones.
- **A different backing store** (e.g. surfacing Slurm jobs once `SlurmExecutor` lands) means another
  discovery source feeding `RunView`-shaped summaries; the HTTP/UI layers are agnostic to where a run
  came from.
