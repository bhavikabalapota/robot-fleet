"""Dynamic discovery and the membership view.

The view is built only from HELLO / HELLO_REPLY traffic, never from a config file.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from . import config
from .util import short

log = logging.getLogger("robot.discovery")

Addr = Tuple[str, int]


@dataclass
class Member:
    robot_id: int
    addr: Addr
    last_seen: float
    tcp_port: Optional[int] = None


class MembershipView:
    def __init__(self, self_id: int, on_change: Optional[Callable[[str, int], None]] = None):
        self.self_id = self_id
        self._members: Dict[int, Member] = {}
        self._lock = threading.Lock()
        self._on_change = on_change

    def touch(self, robot_id: int, addr: Addr, tcp_port: Optional[int] = None) -> None:
        """Record that we heard from robot_id. Adds it if new."""
        if robot_id == self.self_id:
            return
        with self._lock:
            m = self._members.get(robot_id)
            is_new = m is None
            if is_new:
                m = self._members[robot_id] = Member(robot_id, addr, time.monotonic())
            else:
                m.addr = addr
                m.last_seen = time.monotonic()
            if tcp_port:
                m.tcp_port = int(tcp_port)
        if is_new:
            log.info("member JOINED: %s at %s:%s", short(robot_id), addr[0], addr[1])
            if self._on_change:
                self._on_change("join", robot_id)

    def remove(self, robot_id: int) -> None:
        with self._lock:
            removed = self._members.pop(robot_id, None)
        if removed:
            log.info("member LEFT: %s", short(robot_id))
            if self._on_change:
                self._on_change("leave", robot_id)

    def expire(self) -> List[int]:
        now = time.monotonic()
        with self._lock:
            dead = [rid for rid, m in self._members.items()
                    if now - m.last_seen > config.MEMBER_TIMEOUT]
        for rid in dead:
            self.remove(rid)
        return dead

    def addr_of(self, robot_id: int) -> Optional[Addr]:
        with self._lock:
            m = self._members.get(robot_id)
            return m.addr if m else None

    def tcp_addr(self, robot_id: int) -> Optional[Addr]:
        with self._lock:
            m = self._members.get(robot_id)
            return (m.addr[0], m.tcp_port) if m and m.tcp_port else None

    def ids(self) -> List[int]:
        with self._lock:
            return sorted(self._members)

    def snapshot(self) -> List[Member]:
        with self._lock:
            return sorted(self._members.values(), key=lambda m: m.robot_id)
