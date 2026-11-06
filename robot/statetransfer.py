"""State transfer over TCP: a joining robot gets a snapshot from the dispatcher."""
from __future__ import annotations

import json
import logging
import socket
import threading
from typing import Any, Callable, Dict, Optional, Tuple

from . import config, messages as M

log = logging.getLogger("robot.state")


class StateServer:
    def __init__(self, provider: Callable[[], Optional[Dict[str, Any]]]):
        self._provider = provider
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]

    def start(self) -> None:
        threading.Thread(target=self._serve, name="tcp-state", daemon=True).start()

    def stop(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            try:
                conn.settimeout(config.STATE_TIMEOUT)
                req = json.loads(_read_line(conn))
                if req.get("type") != M.STATE_REQUEST:
                    return
                snap = self._provider() or {"type": M.STATE_DENIED}
                conn.sendall(json.dumps(snap).encode() + b"\n")
                if snap["type"] == M.STATE_TRANSFER:
                    log.info("sent state snapshot (%d ops) to a joining robot", len(snap["log"]))
            except (OSError, ValueError) as e:
                log.debug("state transfer failed: %s", e)


def fetch_state(addr: Tuple[str, int]) -> Optional[Dict[str, Any]]:
    try:
        with socket.create_connection(addr, timeout=config.STATE_TIMEOUT) as s:
            s.sendall(json.dumps({"type": M.STATE_REQUEST}).encode() + b"\n")
            resp = json.loads(_read_line(s))
        return resp if resp.get("type") == M.STATE_TRANSFER else None
    except (OSError, ValueError):
        return None


def _read_line(sock: socket.socket) -> str:
    buf = bytearray()
    while not buf.endswith(b"\n"):
        chunk = sock.recv(65536)
        if not chunk:
            break
        buf += chunk
    return buf.decode("utf-8")
