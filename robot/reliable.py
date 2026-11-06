"""Reliable unicast on top of UDP: message IDs, ACK + retransmit, duplicate suppression."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Tuple

from . import config, messages as M
from .transport import Addr, Transport


class ReliableChannel:
    def __init__(self, transport: Transport, self_id: int):
        self._t = transport
        self._id = self_id
        self._lock = threading.Lock()
        self._next_mid = 0
        # mid -> [msg, addr, dest_id, sends, next_resend_time]
        self._pending: Dict[int, list] = {}
        self._seen: "OrderedDict[Tuple[int, int], None]" = OrderedDict()

    def send(self, msg: Dict[str, Any], addr: Addr, dest_id: int) -> None:
        """Send msg until the receiver ACKs it (or MAX_RETRIES is reached)."""
        with self._lock:
            self._next_mid += 1
            msg["mid"] = self._next_mid
            self._pending[self._next_mid] = [msg, addr, dest_id, 1,
                                             time.monotonic() + config.RETRY_INTERVAL]
        self._t.unicast(msg, addr)

    def accept(self, msg: Dict[str, Any], addr: Addr) -> bool:
        """Called for every incoming message. Returns False if it must not be processed
        (it was an ACK, or a duplicate of a reliable message we already processed)."""
        if msg["type"] == M.ACK:
            with self._lock:
                self._pending.pop(int(msg["ack"]), None)
            return False
        mid = msg.get("mid")
        if mid is None:
            return True                       # plain (unreliable) message
        # Always ACK, also duplicates: the previous ACK may have been lost.
        self._t.unicast({"type": M.ACK, "sender": self._id, "port": self._t.port,
                         "ack": mid}, addr)
        key = (int(msg["sender"]), int(mid))
        with self._lock:
            if key in self._seen:
                return False
            self._seen[key] = None
            if len(self._seen) > config.SEEN_LIMIT:
                self._seen.popitem(last=False)
        return True

    def tick(self) -> List[int]:
        """Retransmit due messages. Returns robot IDs that never ACKed (suspected crashed)."""
        now = time.monotonic()
        resend, failed = [], []
        with self._lock:
            for mid, p in list(self._pending.items()):
                if now < p[4]:
                    continue
                if p[3] >= config.MAX_RETRIES:
                    del self._pending[mid]
                    failed.append(p[2])
                    continue
                p[3] += 1
                p[4] = now + config.RETRY_INTERVAL
                resend.append((p[0], p[1]))
        for msg, addr in resend:
            self._t.unicast(msg, addr)
        return list(dict.fromkeys(failed))
