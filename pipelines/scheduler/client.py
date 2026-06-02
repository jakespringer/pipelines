"""Client side of the run-server API.

:class:`RunClient` connects to a server port, performs the ``hello`` handshake to receive
the initial snapshot, then streams live event records to a callback on a background thread.
Control commands (``cancel``/``hold``/``release``) are written on the same socket. The
:mod:`tui` is the main consumer, but this is a plain object usable from any script.
"""

from __future__ import annotations

import socket
import threading
from typing import Callable

from . import protocol, registry


class RunClient:
    def __init__(self, port: int, host: str = "127.0.0.1"):
        self.port = port
        self.host = host
        self.sock: socket.socket | None = None
        self._fh = None
        self._send_lock = threading.Lock()
        self._reader: threading.Thread | None = None
        self._closed = False

    # --- connection -------------------------------------------------------- #
    def connect(self) -> dict:
        """Connect and return the initial snapshot message."""
        self.sock = socket.create_connection((self.host, self.port), timeout=5.0)
        self.sock.settimeout(None)
        self._fh = protocol.reader(self.sock)
        protocol.send_msg(self.sock, {"cmd": "hello"}, self._send_lock)
        snapshot = protocol.next_msg(self._fh)
        if snapshot is None:
            raise ConnectionError("server closed before sending a snapshot")
        return snapshot

    def stream(self, on_event: Callable[[dict], None]) -> None:
        """Spawn a daemon thread that calls ``on_event`` for every streamed record."""
        def _loop():
            try:
                for msg in protocol.iter_msgs(self._fh):
                    on_event(msg)
            except OSError:
                pass
            finally:
                self._closed = True
                on_event({"type": "_disconnected"})
        self._reader = threading.Thread(target=_loop, daemon=True)
        self._reader.start()

    @property
    def closed(self) -> bool:
        return self._closed

    # --- commands ---------------------------------------------------------- #
    def _send(self, obj: dict) -> None:
        if self.sock is None:
            return
        try:
            protocol.send_msg(self.sock, obj, self._send_lock)
        except OSError:
            self._closed = True

    def cancel(self, relpath: str) -> None:
        self._send({"cmd": "cancel", "relpath": relpath})

    def hold(self, relpath: str) -> None:
        self._send({"cmd": "hold", "relpath": relpath})

    def release(self, relpath: str) -> None:
        self._send({"cmd": "release", "relpath": relpath})

    def close(self) -> None:
        self._closed = True
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass


def resolve_port(port: int | None) -> int:
    """Resolve an explicit port, or discover the newest live run from the registry."""
    if port is not None:
        if not registry.port_alive(port):
            raise ConnectionError(f"no live pipelines run on port {port}")
        return port
    run = registry.newest_alive()
    if run is None:
        raise ConnectionError("no live pipelines runs found (start one with `pipelines runparallel`)")
    return int(run["port"])
