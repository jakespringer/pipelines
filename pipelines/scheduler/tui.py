"""``pipelines attach`` — a curses monitor for a parallel run.

Layout (selected job's output on top; a status panel pinned at the bottom):

    ┌─────────────────────────────────────────────────────────┐
    │ <selected job's full output — no border, fills the pane> │   ← output pane
    │ ... scroll: ↑/↓ line, PgUp/PgDn page, End/g bottom ...    │
    ├─────────────────────────────────────────────────────────  ← status line (white on black)
    │ [0:03:12] run 4/20  ●3 ◐1 ○12 ✓4 ✗0   gpu 5/8 cpu 20/32   │
    │ judged/…/strict   2:41                ← selected job: full name + elapsed │
    │ ● running (3)   Generations#0  Generations#1  Judged#0    │   ← one row per state
    │ ○ queued (12)   Judged#1  Judged#2  Judged#3  +9          │
    │ ✓ completed (4) Index  Preview#0  Preview#1  Generations#2 │
    └─────────────────────────────────────────────────────────┘

Each job is named by its artifact class plus an index (only shown when more than one job
shares that class), so the strip reads as job *types* rather than long paths. Jobs are grouped
into one row per state; the selected job's full relpath and elapsed time print just above.

Keys:
  ← / →      move within the current state row (between jobs of that state)
  ↑ / ↓      move between state rows (running ↔ yielding ↔ queued ↔ …)
  Tab        jump to the next state row (wraps)
  PgUp/PgDn  scroll the output one page    Home  top of output
  End / g    jump to the bottom of the output (re-enable autoscroll)
  c          cancel selected job          h  hold        r  release
  q          quit the monitor (the run keeps going)

The output pane has no border so terminal copy-paste grabs clean text. Live data arrives on a
background stream thread; the main thread only draws and handles keys.
"""

from __future__ import annotations

import curses
import os
import threading
import time

from .client import RunClient, resolve_port

# state -> (priority for strip ordering, glyph, color-pair name)
_STATES = {
    "running":   (0, "●", "running"),    # ●
    "yielding":  (1, "◐", "yielding"),   # ◐
    "queued":    (2, "○", "queued"),     # ○
    "held":      (3, "‖", "held"),       # ‖
    "blocked":   (4, "·", "blocked"),    # ·
    "completed": (5, "✓", "completed"),  # ✓
    "failed":    (6, "✗", "failed"),     # ✗
    "cancelled": (7, "⊘", "failed"),     # ⊘
}
_DEFAULT = (9, "?", "queued")


class Model:
    """Thread-safe view of the run, updated from the server event stream."""

    def __init__(self, snapshot: dict):
        self._lock = threading.Lock()
        self.connected = True
        self.apply_snapshot(snapshot)

    def apply_snapshot(self, snap: dict) -> None:
        with self._lock:
            self.logdir = snap.get("logdir", "")
            self.started_at = snap.get("started_at", time.time())
            self.pool = snap.get("pool", {})
            self.order = [j["relpath"] for j in snap.get("jobs", [])]
            self.jobs = {j["relpath"]: j for j in snap.get("jobs", [])}

    def on_event(self, ev: dict) -> None:
        t = ev.get("type")
        with self._lock:
            if t == "snapshot":
                # re-entrant; release lock first
                pass
            elif t == "job_state":
                rp = ev.get("relpath")
                if rp is not None:
                    if rp not in self.jobs:
                        self.order.append(rp)
                    self.jobs[rp] = ev
            elif t == "pool":
                self.pool = {k: ev[k] for k in ("gpus", "cpus", "memory_mb") if k in ev}
            elif t == "_disconnected":
                self.connected = False
        if t == "snapshot":
            self.apply_snapshot(ev)

    # --- reads (copy out under lock) --------------------------------------- #
    def display_order(self) -> list[dict]:
        """Jobs grouped by state (running first … completed/failed last). Within a
        state the order is declaration order, except ``completed`` jobs, which read
        newest-first by completion time so the most recently finished sits at the
        front of the row."""
        with self._lock:
            jobs = [self.jobs[r] for r in self.order if r in self.jobs]
            order_index = {r: i for i, r in enumerate(self.order)}

        def key(j: dict):
            prio = _STATES.get(j["state"], _DEFAULT)[0]
            if j["state"] == "completed":
                ended = j.get("ended_at")               # set by the server on done
                # Negate so most-recent sorts first; untimed jobs sink to the end.
                return (prio, ended is None, -(ended or 0.0))
            return (prio, False, order_index[j["relpath"]])

        return sorted(jobs, key=key)

    def stable_order(self) -> list[dict]:
        """Jobs in their original (declaration) order — used for stable per-class indices."""
        with self._lock:
            return [dict(self.jobs[r]) for r in self.order if r in self.jobs]

    def get_job(self, relpath: str | None) -> dict:
        with self._lock:
            return dict(self.jobs.get(relpath, {})) if relpath else {}

    def counts(self) -> dict:
        with self._lock:
            c: dict[str, int] = {}
            for r in self.order:
                st = self.jobs[r]["state"]
                c[st] = c.get(st, 0) + 1
            return c

    def snapshot_meta(self) -> tuple:
        with self._lock:
            return self.started_at, dict(self.pool), self.connected, len(self.order)


class App:
    def __init__(self, stdscr, client: RunClient, model: Model):
        self.scr = stdscr
        self.client = client
        self.model = model
        self.selected: str | None = None
        self.out_scroll = 0
        self.autoscroll = True
        self._out_cache: dict[str, tuple] = {}   # relpath -> (size, lines)
        self._panel_h = 2            # height of the bottom status panel (recomputed each draw)

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        curses.curs_set(0)
        self.scr.timeout(200)
        _init_colors()
        while True:
            self._draw()
            try:
                ch = self.scr.getch()
            except KeyboardInterrupt:
                break
            if ch == -1:
                continue
            if not self._handle_key(ch):
                break

    # ------------------------------------------------------------------ #
    # Input
    # ------------------------------------------------------------------ #
    def _handle_key(self, ch: int) -> bool:
        grouped = self._grouped()
        gi, wi = self._locate(grouped)
        if ch in (ord("q"),):
            return False
        elif ch == curses.KEY_LEFT:                    # move within the current state row
            self._move(grouped, gi, wi - 1)
        elif ch == curses.KEY_RIGHT:
            self._move(grouped, gi, wi + 1)
        elif ch == curses.KEY_UP:                      # move between state rows (types)
            self._move(grouped, gi - 1, wi)
        elif ch == curses.KEY_DOWN:
            self._move(grouped, gi + 1, wi)
        elif ch == ord("\t"):                          # next state row (wraps)
            self._move(grouped, (gi + 1) % len(grouped) if grouped else 0, wi)
        elif ch == curses.KEY_PPAGE:
            self.autoscroll = False
            self.out_scroll = max(0, self.out_scroll - self._out_height())
        elif ch == curses.KEY_NPAGE:
            self.out_scroll += self._out_height()
        elif ch == curses.KEY_HOME:
            self.autoscroll = False
            self.out_scroll = 0
        elif ch in (curses.KEY_END, ord("g")):
            self.autoscroll = True
        elif ch == ord("c") and self.selected:
            self.client.cancel(self.selected)
        elif ch == ord("h") and self.selected:
            self.client.hold(self.selected)
        elif ch == ord("r") and self.selected:
            self.client.release(self.selected)
        elif ch == curses.KEY_RESIZE:
            pass
        return True

    def _grouped(self) -> list[tuple[str, list[dict]]]:
        """Jobs as [(state, [jobs])] in row order — the same grouping the panel draws."""
        groups: dict[str, list[dict]] = {}
        for j in self.model.display_order():
            groups.setdefault(j["state"], []).append(j)
        return list(groups.items())

    def _locate(self, grouped: list[tuple[str, list[dict]]]) -> tuple[int, int]:
        """(row, column) of the selected job within ``grouped``; (0, 0) if not found."""
        for gi, (_, grp) in enumerate(grouped):
            for wi, j in enumerate(grp):
                if j["relpath"] == self.selected:
                    return gi, wi
        return 0, 0

    def _move(self, grouped: list[tuple[str, list[dict]]], gi: int, wi: int) -> None:
        """Select the job at row ``gi`` column ``wi``, clamping both into range."""
        if not grouped:
            return
        gi = max(0, min(len(grouped) - 1, gi))
        grp = grouped[gi][1]
        wi = max(0, min(len(grp) - 1, wi))
        self.selected = grp[wi]["relpath"]
        self.autoscroll = True
        self.out_scroll = 0

    def _selected_index(self, jobs: list[dict]) -> int:
        for i, j in enumerate(jobs):
            if j["relpath"] == self.selected:
                return i
        if jobs:                                       # default to first
            self.selected = jobs[0]["relpath"]
        return 0

    # ------------------------------------------------------------------ #
    # Drawing
    # ------------------------------------------------------------------ #
    def _out_height(self) -> int:
        return max(1, self.scr.getmaxyx()[0] - self._panel_h)

    def _draw(self) -> None:
        self.scr.erase()
        h, w = self.scr.getmaxyx()
        jobs = self.model.display_order()
        self._selected_index(jobs)                     # ensures self.selected valid
        # Group jobs into one row per state (display_order is already state-sorted).
        groups: dict[str, list[dict]] = {}
        for j in jobs:
            groups.setdefault(j["state"], []).append(j)
        # status + detail + log path + one row per state
        self._panel_h = min(h - 1, 3 + len(groups))
        out_h = max(1, h - self._panel_h)
        labels = self._build_labels()
        self._draw_output(out_h, w)
        self._draw_status(out_h, w)
        self._draw_detail(out_h + 1, w, labels)
        self._draw_logpath(out_h + 2, w)
        for i, (state, group) in enumerate(groups.items()):
            self._draw_state_row(out_h + 3 + i, w, state, group, labels)
        self.scr.noutrefresh()
        curses.doupdate()

    # Map each relpath to a short display name: artifact class, plus a #index when more
    # than one job shares that class. Indices follow declaration order so they're stable.
    def _build_labels(self) -> dict[str, str]:
        order = self.model.stable_order()
        totals: dict[str, int] = {}
        for j in order:
            totals[_job_type(j)] = totals.get(_job_type(j), 0) + 1
        seen: dict[str, int] = {}
        labels: dict[str, str] = {}
        for j in order:
            t = _job_type(j)
            i = seen.get(t, 0)
            seen[t] = i + 1
            labels[j["relpath"]] = t if totals[t] == 1 else f"{t}#{i}"
        return labels

    def _draw_output(self, out_h: int, w: int) -> None:
        lines = self._output_lines()
        max_scroll = max(0, len(lines) - out_h)
        if self.autoscroll:
            self.out_scroll = max_scroll
        else:
            self.out_scroll = min(self.out_scroll, max_scroll)
            if self.out_scroll >= max_scroll:
                self.autoscroll = True
        view = lines[self.out_scroll:self.out_scroll + out_h]
        for row, line in enumerate(view):
            _addstr(self.scr, row, 0, line[:w - 1])

    def _output_lines(self) -> list[str]:
        if not self.selected:
            return ["(no job selected)"]
        job = self.model.get_job(self.selected)
        path = job.get("log_path")
        if not path or not os.path.exists(path):
            return [f"({self.selected}: {job.get('state', '?')} — no output yet)"]
        try:
            size = os.path.getsize(path)
        except OSError:
            return ["(output unavailable)"]
        cached = self._out_cache.get(self.selected)
        if cached and cached[0] == size:
            return cached[1]
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines() or ["(empty)"]
        except OSError:
            return ["(output unavailable)"]
        self._out_cache[self.selected] = (size, lines)
        return lines

    def _draw_status(self, y: int, w: int) -> None:
        started, pool, connected, total = self.model.snapshot_meta()
        c = self.model.counts()
        finished = c.get("completed", 0) + c.get("failed", 0) + c.get("cancelled", 0)
        elapsed = _fmt_dur(time.time() - started)
        gpu = pool.get("gpus", {})
        cpu = pool.get("cpus", {})
        seg = (f" [{elapsed}] run {finished}/{total}  "
               f"●{c.get('running',0)} ◐{c.get('yielding',0)} "
               f"○{c.get('queued',0)+c.get('held',0)} "
               f"✓{c.get('completed',0)} ✗{c.get('failed',0)}  ")
        if gpu:
            seg += f"gpu {gpu.get('free','?')}/{gpu.get('total','?')} "
        if cpu:
            seg += f"cpu {cpu.get('free','?')}/{cpu.get('total','?')} "
        if not connected:
            seg += " [disconnected]"
        seg = seg.ljust(w)[:w]
        _addstr(self.scr, y, 0, seg, curses.color_pair(_PAIR.get("status", 0)) | curses.A_BOLD)

    def _draw_detail(self, y: int, w: int, labels: dict[str, str]) -> None:
        """Row with the selected job's short name, full relpath and elapsed time."""
        job = self.model.get_job(self.selected)
        if not job:
            _addstr(self.scr, y, 0, " (no job selected)".ljust(w)[:w])
            return
        name = labels.get(self.selected, self.selected)
        elapsed = _job_elapsed(job)
        el = _fmt_dur(elapsed) if elapsed is not None else "—"
        attr = _state_attr(job.get("state", ""))
        _addstr(self.scr, y, 0, " " * w)                # clear the row first
        _addstr(self.scr, y, 1, name, attr | curses.A_BOLD)
        x = 1 + len(name) + 2
        rp = self.selected or ""
        avail = max(0, w - 1 - x - len(el) - 2)
        if len(rp) > avail:                             # keep the tail (most specific part)
            rp = "…" + rp[-(avail - 1):] if avail > 1 else ""
        _addstr(self.scr, y, x, rp)
        _addstr(self.scr, y, max(x, w - 1 - len(el)), el, curses.A_BOLD)

    def _draw_logpath(self, y: int, w: int) -> None:
        """Row showing the on-disk log file the selected job's output is streamed to."""
        job = self.model.get_job(self.selected)
        path = job.get("log_path") if job else None
        label = "log: "
        _addstr(self.scr, y, 0, " " * w)                # clear the row first
        _addstr(self.scr, y, 1, label, curses.A_DIM)
        if not path:
            _addstr(self.scr, y, 1 + len(label), "(none)", curses.A_DIM)
            return
        x = 1 + len(label)
        avail = max(0, w - 1 - x)
        if len(path) > avail:                           # keep the tail (the filename)
            path = "…" + path[-(avail - 1):] if avail > 1 else ""
        _addstr(self.scr, y, x, path)

    def _draw_state_row(self, y: int, w: int, state: str, group: list[dict],
                        labels: dict[str, str]) -> None:
        """One row for a single state: a colored header, then its jobs as compact tabs."""
        attr = _state_attr(state)
        glyph = _STATES.get(state, _DEFAULT)[1]
        header = f" {glyph} {state} ({len(group)}) "
        _addstr(self.scr, y, 0, header, attr | curses.A_BOLD)
        x0 = len(header)
        avail = w - 1 - x0
        if avail <= 0:
            return
        labs = [labels.get(j["relpath"], j["relpath"]) for j in group]
        focus = next((i for i, j in enumerate(group)
                      if j["relpath"] == self.selected), None)
        start, placed = _fit_labels(labs, avail, focus)
        x = x0
        for i in placed:
            a = attr
            if group[i]["relpath"] == self.selected:
                a = curses.color_pair(_PAIR.get("selected", 0)) | curses.A_REVERSE | curses.A_BOLD
            _addstr(self.scr, y, x, labs[i], a)
            x += len(labs[i]) + 1
        hidden = len(labs) - len(placed)
        if hidden > 0:                                  # show how many didn't fit
            tag = f"+{hidden}"
            _addstr(self.scr, y, min(x, w - 1 - len(tag)), tag, attr)


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
_PAIR: dict[str, int] = {}


def _init_colors() -> None:
    try:
        curses.start_color()
        curses.use_default_colors()
    except curses.error:
        return
    spec = {
        "running": curses.COLOR_GREEN, "yielding": curses.COLOR_YELLOW,
        "queued": curses.COLOR_CYAN, "held": curses.COLOR_MAGENTA,
        "blocked": curses.COLOR_WHITE, "completed": curses.COLOR_BLUE,
        "failed": curses.COLOR_RED,
    }
    n = 1
    for name, color in spec.items():
        try:
            curses.init_pair(n, color, -1)
        except curses.error:
            pass
        _PAIR[name] = n
        n += 1
    # status bar: white text on a black background
    try:
        curses.init_pair(n, curses.COLOR_WHITE, curses.COLOR_BLACK)
    except curses.error:
        pass
    _PAIR["status"] = n
    n += 1
    try:
        curses.init_pair(n, curses.COLOR_BLACK, curses.COLOR_CYAN)
    except curses.error:
        pass
    _PAIR["selected"] = n


def _state_attr(state: str):
    name = _STATES.get(state, _DEFAULT)[2]
    return curses.color_pair(_PAIR.get(name, 0))


def _job_type(job: dict) -> str:
    """The job's display class: artifact class name, else the leading relpath segment."""
    cls = job.get("cls")
    if cls:
        return cls
    return job.get("relpath", "?").split("/")[0] or job.get("relpath", "?")


def _job_elapsed(job: dict) -> float | None:
    """Seconds the job has run: now-started while live, ended-started once finished."""
    started = job.get("started_at")
    if not started:
        return None
    return (job.get("ended_at") or time.time()) - started


def _fit_labels(labs: list[str], avail: int, focus: int | None) -> tuple[int, list[int]]:
    """Pick a contiguous window of label indices that fits in ``avail`` columns.

    Packs greedily from ``start``; if ``focus`` would fall outside the window, shift the
    start forward until it fits. Returns ``(start, placed_indices)``.
    """
    start = 0
    while True:
        x, placed = 0, []
        for i in range(start, len(labs)):
            need = len(labs[i]) + 1
            if x + need > avail and placed:
                break
            placed.append(i)
            x += need
        if focus is None or not placed or focus in placed or start >= len(labs) - 1:
            return start, placed
        start += 1


def _addstr(scr, y: int, x: int, text: str, attr=0) -> None:
    try:
        scr.addstr(y, x, text, attr)
    except curses.error:
        pass                                            # writing the last cell raises; ignore


def _fmt_dur(seconds: float) -> str:
    s = int(max(0, seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}"


def attach_main(args: list[str]) -> int:
    """Entry point for ``pipelines attach [PORT]``."""
    port = None
    for a in args:
        if a.isdigit():
            port = int(a)
    try:
        port = resolve_port(port)
    except ConnectionError as e:
        print(f"pipelines attach: {e}")
        return 2
    client = RunClient(port)
    try:
        snapshot = client.connect()
    except (OSError, ConnectionError) as e:
        print(f"pipelines attach: could not connect to port {port}: {e}")
        return 2
    model = Model(snapshot)
    client.stream(model.on_event)

    def _run(stdscr):
        App(stdscr, client, model).run()

    try:
        curses.wrapper(_run)
    finally:
        client.close()
    return 0
