"""Newline-delimited JSON framing over a socket.

Both directions speak one JSON object per line. This is the entire wire format between the
run :mod:`server` and any client (the :mod:`tui`, a script, or a worker sending a yield).
Deliberately trivial so new message types are just new dict keys.

Reads go through a single long-lived file object per socket (``reader``); creating two
makefiles on one socket can drop bytes buffered in the first, so callers keep one ``reader``
for a connection's whole lifetime.
"""

from __future__ import annotations

import json
import socket
import threading
from typing import Iterator, TextIO


def send_msg(sock: socket.socket, obj: dict, lock: threading.Lock | None = None) -> None:
    """Send one JSON object as a line. ``lock`` serializes concurrent writers on a socket."""
    data = (json.dumps(obj) + "\n").encode("utf-8")
    if lock is not None:
        with lock:
            sock.sendall(data)
    else:
        sock.sendall(data)


def reader(sock: socket.socket) -> TextIO:
    """One persistent line reader for the lifetime of ``sock``."""
    return sock.makefile("r", encoding="utf-8")


def next_msg(fh: TextIO) -> dict | None:
    """Read one JSON message, or ``None`` on EOF / timeout. Skips blank/garbage lines."""
    while True:
        try:
            line = fh.readline()
        except (socket.timeout, TimeoutError, OSError):
            return None
        if line == "":
            return None                       # EOF
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue


def iter_msgs(fh: TextIO) -> Iterator[dict]:
    """Yield decoded JSON objects until the connection closes."""
    while True:
        msg = next_msg(fh)
        if msg is None:
            return
        yield msg
