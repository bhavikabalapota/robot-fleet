"""Bully election extended with epochs.

- ELECTION goes to every robot with a higher ID.
- A higher robot replies ANSWER and starts its own election.
- No ANSWER within 2 s -> we win and become dispatcher (new epoch).
- ANSWER but no COORDINATOR within 5 s -> start again.
All methods are called with the robot lock held.
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Dict

from . import config, messages as M
from .transport import Addr
from .util import short

if TYPE_CHECKING:
    from .node import Robot

log = logging.getLogger("robot.election")


class Election:
    def __init__(self, robot: "Robot"):
        self.r = robot
        self.running = False
        self.waiting_for_coordinator = False
        self.deadline = 0.0

    def start(self) -> None:
        if self.running:
            return
        r = self.r
        self.running = True
        self.waiting_for_coordinator = False
        higher = [m for m in r.view.ids() if m > r.id]
        if not higher:
            log.warning("ELECTION: no robot with a higher ID -> I win")
            self._win()
            return
        log.warning("ELECTION started, asking higher IDs %s", [short(m) for m in higher])
        for m in higher:
            addr = r.view.addr_of(m)
            if addr:
                r.reliable.send(r.envelope(M.ELECTION), addr, m)
        self.deadline = time.monotonic() + config.ELECTION_ANSWER_TIMEOUT

    def on_election(self, msg: Dict[str, Any], addr: Addr) -> None:
        sender = int(msg["sender"])
        if sender >= self.r.id:
            return
        self.r.reliable.send(self.r.envelope(M.ANSWER), addr, sender)
        if self.r.is_dispatcher:
            # Already the dispatcher (e.g. a late ELECTION after we won): just tell the
            # sender instead of running a new election and bumping the epoch again.
            self.r.reliable.send(self.r.envelope(M.COORDINATOR,
                                                 last_seq=self.r.order.last_delivered),
                                 addr, sender)
            return
        self.start()                      # take over the election

    def on_answer(self, msg: Dict[str, Any]) -> None:
        if self.running and not self.waiting_for_coordinator:
            log.info("got ANSWER from %s, waiting for COORDINATOR", short(int(msg["sender"])))
            self.waiting_for_coordinator = True
            self.deadline = time.monotonic() + config.COORDINATOR_TIMEOUT

    def cancel(self) -> None:
        self.running = False
        self.waiting_for_coordinator = False

    def tick(self) -> None:
        if not self.running or time.monotonic() < self.deadline:
            return
        if self.waiting_for_coordinator:
            log.warning("no COORDINATOR within %.0fs -> restarting election",
                        config.COORDINATOR_TIMEOUT)
            self.running = False
            self.start()
        else:
            log.warning("no ANSWER within %.0fs -> I win", config.ELECTION_ANSWER_TIMEOUT)
            self._win()

    def _win(self) -> None:
        self.running = False
        self.r.become_dispatcher()
