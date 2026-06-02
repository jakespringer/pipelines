"""``pipelines dashboard`` — a small, dependency-free web monitor for parallel runs.

A :class:`~http.server.ThreadingHTTPServer` serves a single-page UI plus a JSON/SSE API backed by
:class:`pipelines.dashboard.index.RunIndex`. It needs no project, no run server connection, and no
third-party packages — it just watches the registry directory, so newly launched ``runparallel``
servers (one or many) show up on their own within a poll, and finished runs stay browsable.

Routes
------
``GET /``                                   the dashboard page (static assets)
``GET /api/runs``                           JSON list of every run + summary
``GET /api/overview``                       JSON every run's full detail (the expanded view)
``GET /api/runs/<port>``                    JSON detail of one run, grouped into pipeline steps
``GET /api/runs/<port>/stream``             SSE: a snapshot, then live event records
``GET /api/runs/<port>/log/<slug>``         a job's full log as text/plain
``GET /api/runs/<port>/log/<slug>/stream``  SSE: the log, then live-tailed appends

Live data uses Server-Sent Events. Every SSE payload is a JSON value (so log text with arbitrary
newlines never collides with SSE's line framing). Each long-lived stream runs on its own thread and
notices client disconnects on the next write, so nothing leaks when a tab closes.

Extending it: add a branch in :meth:`_DashboardHandler._route`. Control actions (cancel/hold) would
be ``POST`` routes that open a :class:`pipelines.scheduler.client.RunClient` to the run's live port;
the read path here is deliberately decoupled from that so the monitor keeps working for dead runs.
"""

from __future__ import annotations

import json
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from ..identity import validate_under_base
from ..scheduler import registry
from .index import RunIndex, RunView

ASSETS = Path(__file__).parent / "assets"

_POLL = 0.4          # seconds between event-log reads on a live stream
_LOG_POLL = 0.3      # seconds between size checks on a live log tail
_HEARTBEAT = 15.0    # seconds of quiet before an SSE keepalive comment
_CHUNK = 256 * 1024  # max bytes of log read per tail iteration

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
}


class _DashboardHandler(BaseHTTPRequestHandler):
    server_version = "pipelines-dashboard"
    protocol_version = "HTTP/1.1"     # keep-alive for the many small API calls

    # -- plumbing ----------------------------------------------------------- #
    @property
    def _index(self) -> RunIndex:
        return self.server.run_index           # set in _make_server

    def log_message(self, *args) -> None:      # keep the console to our own startup lines
        pass

    def do_GET(self) -> None:
        try:
            self._route()
        except (BrokenPipeError, ConnectionResetError):
            pass                                # client went away mid-response; nothing to do

    def _route(self) -> None:
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)

        if path in ("/", "/index.html"):
            return self._asset("index.html")
        if path in ("/app.js", "/style.css", "/favicon.svg"):
            return self._asset(path.lstrip("/"))
        if path == "/favicon.ico":
            return self._reply(b"", "image/x-icon", code=204)

        if path == "/api/runs":
            return self._json(self._index.index_payload())
        if path == "/api/overview":
            return self._json(self._index.overview_payload())

        parts = [p for p in path.split("/") if p]
        if len(parts) >= 3 and parts[:2] == ["api", "runs"] and parts[2].isdigit():
            port, rest = int(parts[2]), parts[3:]
            if not rest:
                payload = self._index.detail_payload(port)
                return self._json(payload) if payload else self._not_found()
            if rest == ["stream"]:
                return self._stream_run(port)
            if rest[0] == "log" and len(rest) >= 2:
                slug = rest[1]
                if rest[2:] == ["stream"]:
                    return self._stream_log(port, slug)
                if len(rest) == 2:
                    query = parse_qs(parsed.query)
                    return self._serve_log(port, slug, download="download" in query)
        self._not_found()

    # -- finite responses --------------------------------------------------- #
    def _reply(self, body: bytes, ctype: str, code: int = 200, extra: dict | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj, code: int = 200) -> None:
        self._reply(json.dumps(obj).encode("utf-8"), "application/json; charset=utf-8", code)

    def _asset(self, name: str) -> None:
        path = ASSETS / name
        ctype = _CONTENT_TYPES.get(path.suffix, "application/octet-stream")
        try:
            body = path.read_bytes()
        except OSError:
            return self._not_found()
        self._reply(body, ctype, extra={"Cache-Control": "no-cache"})

    def _not_found(self) -> None:
        self._reply(b"not found", "text/plain; charset=utf-8", code=404)

    def _serve_log(self, port: int, slug: str, *, download: bool) -> None:
        path = self._log_path(port, slug)
        if path is None:
            return self._not_found()
        try:
            body = path.read_bytes()
        except OSError:
            body = b""
        extra = {"Content-Disposition": f'attachment; filename="{slug}.log"'} if download else None
        self._reply(body, "text/plain; charset=utf-8", extra=extra)

    def _log_path(self, port: int, slug: str) -> Path | None:
        logdir = self._index.logdir_for(port)
        if logdir is None or "/" in slug or slug in ("", ".", ".."):
            return None
        jobs_dir = Path(logdir) / "jobs"
        candidate = jobs_dir / f"{slug}.log"
        try:
            validate_under_base(candidate, jobs_dir)
        except Exception:
            return None
        return candidate

    # -- SSE plumbing ------------------------------------------------------- #
    def _sse_open(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")    # don't let a proxy buffer the stream
        self.end_headers()
        self.close_connection = True

    def _sse(self, payload: str) -> bool:
        try:
            self.wfile.write(payload.encode("utf-8"))
            self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            return False                                # client disconnected; stop the stream

    def _emit(self, obj, event: str | None = None) -> bool:
        head = f"event: {event}\n" if event else ""
        return self._sse(f"{head}data: {json.dumps(obj)}\n\n")

    def _beat(self) -> bool:
        return self._sse(": keepalive\n\n")

    # -- run stream --------------------------------------------------------- #
    def _stream_run(self, port: int) -> None:
        logdir = self._index.logdir_for(port)
        if logdir is None:
            return self._not_found()
        self._sse_open()
        view = RunView(port, logdir)
        view.refresh()
        live = registry.port_alive(port)
        info = self._index.registry_info(port)
        if not self._emit(self._index.detail_from_view(view, live, info), event="snapshot"):
            return
        quiet = time.monotonic()
        while True:
            records = view.refresh()
            for record in records:                     # forward each new event verbatim
                if not self._emit(record):
                    return
            if records:
                quiet = time.monotonic()
            live = False if view.done else registry.port_alive(port)
            if view.done or not live:
                self._emit({"done": view.done, "ok": view.ok}, event="end")
                return
            if time.monotonic() - quiet > _HEARTBEAT:
                if not self._beat():
                    return
                quiet = time.monotonic()
            time.sleep(_POLL)

    # -- log stream --------------------------------------------------------- #
    def _stream_log(self, port: int, slug: str) -> None:
        path = self._log_path(port, slug)
        if path is None:
            return self._not_found()
        self._sse_open()
        offset = 0
        quiet = time.monotonic()
        while True:
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            if size < offset:                          # truncated: tell the client to clear
                offset = 0
                if not self._emit({}, event="reset"):
                    return
            if size > offset:
                try:
                    with path.open("rb") as fh:
                        fh.seek(offset)
                        chunk = fh.read(_CHUNK)
                        offset = fh.tell()
                except OSError:
                    chunk = b""
                if chunk:
                    if not self._emit(chunk.decode("utf-8", "replace")):
                        return
                    quiet = time.monotonic()
                    continue                           # there may be more buffered; read again
            if not registry.port_alive(port):          # run finished and we are caught up
                self._emit({}, event="end")
                return
            if time.monotonic() - quiet > _HEARTBEAT:
                if not self._beat():
                    return
                quiet = time.monotonic()
            time.sleep(_LOG_POLL)


# --------------------------------------------------------------------------- #
# Server lifecycle + CLI entry point
# --------------------------------------------------------------------------- #
def _make_server(host: str, port: int) -> ThreadingHTTPServer:
    """Bind ``host:port``, scanning upward for a free port if it is taken."""
    last: OSError | None = None
    for candidate in range(port, port + 50):
        try:
            httpd = ThreadingHTTPServer((host, candidate), _DashboardHandler)
        except OSError as exc:
            last = exc
            continue
        httpd.daemon_threads = True
        httpd.run_index = RunIndex()
        return httpd
    raise SystemExit(f"pipelines dashboard: no free port in [{port}, {port + 50}): {last}")


def serve(host: str = "127.0.0.1", port: int = 7000, *, open_browser: bool = False) -> int:
    httpd = _make_server(host, port)
    bound = httpd.server_address[1]
    shown = host if host not in ("0.0.0.0", "") else "localhost"
    url = f"http://{shown}:{bound}/"
    print(f"pipelines dashboard: serving on {url}")
    print(f"pipelines dashboard: watching {registry.registry_dir()}")
    if bound != port:
        print(f"pipelines dashboard: (port {port} was busy)")
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\npipelines dashboard: stopped")
    finally:
        httpd.shutdown()
        httpd.server_close()
    return 0


def dashboard_main(args: list[str]) -> int:
    """Entry point for ``pipelines dashboard [--port N] [--host H] [--open]``."""
    host, port, open_browser = "127.0.0.1", 7000, False
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--port" and i + 1 < len(args):
            port, i = _int(args[i + 1], port), i + 2
        elif arg.startswith("--port="):
            port, i = _int(arg.split("=", 1)[1], port), i + 1
        elif arg == "--host" and i + 1 < len(args):
            host, i = args[i + 1], i + 2
        elif arg.startswith("--host="):
            host, i = arg.split("=", 1)[1], i + 1
        elif arg in ("--open", "--browser"):
            open_browser, i = True, i + 1
        elif arg in ("--all-interfaces", "--public"):
            host, i = "0.0.0.0", i + 1
        else:
            i += 1
    return serve(host, port, open_browser=open_browser)


def _int(value: str, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback
