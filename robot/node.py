"""A robot. Every robot runs this same code; any robot can become the dispatcher.

Threads: two UDP receive threads, one timer thread ("tick"), a TCP server, and a
state-fetch thread while joining. All robot state is protected by one RLock.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from . import config, messages as M
from .discovery import MembershipView
from .election import Election
from .ordering import TotalOrder
from .reliable import ReliableChannel
from .statetransfer import StateServer, fetch_state
from .tasks import Scorer, TaskBoard
from .transport import Addr, Transport
from .util import short

log = logging.getLogger("robot")


class Robot:
    def __init__(self, robot_id: Optional[int] = None, loss: float = 0.0,
                 pos: Optional[Tuple[float, float]] = None):
        # Random 128-bit ID from a UUID4; kept as int so Bully can compare IDs.
        self.id: int = robot_id if robot_id is not None else uuid.uuid4().int
        self.epoch = 0
        self.max_epoch_seen = 0
        self.dispatcher: Optional[int] = None
        self.syncing = False               # new dispatcher collecting SYNC_REPLYs

        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._got_reply = threading.Event()
        self._fetching = False

        self.view = MembershipView(self.id)
        self.transport = Transport(self._on_message, loss=loss)
        self.reliable = ReliableChannel(self.transport, self.id)
        self.state_server = StateServer(self._snapshot_for_joiner)
        self.order = TotalOrder()
        self.board = TaskBoard()
        self.scorer = Scorer(pos)
        self.election = Election(self)

        now = time.monotonic()
        self._last_hb = now                # last heartbeat heard from the dispatcher
        self._last_hb_sent = 0.0
        self._last_hello = now
        self._last_nack = 0.0
        self._last_resubmit = 0.0
        self._sync_replies: Dict[int, Dict[str, Any]] = {}
        self._sync_expected: set = set()
        self._sync_deadline = 0.0
        self._task_counter = 0

        self._handlers = {
            M.HELLO: self._on_hello, M.HELLO_REPLY: self._on_hello_reply,
            M.HEARTBEAT: self._on_heartbeat, M.ELECTION: self._on_election,
            M.ANSWER: self._on_answer, M.COORDINATOR: self._on_coordinator,
            M.SYNC_REPLY: self._on_sync_reply, M.REQUEST: self._on_request,
            M.ORDERED: self._on_ordered, M.NACK: self._on_nack,
        }

    # ================================================================== lifecycle
    @property
    def is_dispatcher(self) -> bool:
        return self.dispatcher == self.id

    def start(self) -> None:
        log.info("robot %s starting (udp %s, tcp %s, position %.0f,%.0f)", short(self.id),
                 self.transport.port, self.state_server.port, self.scorer.x, self.scorer.y)
        self.transport.start()
        self.state_server.start()
        threading.Thread(target=self._tick_loop, name="tick", daemon=True).start()
        self._join()

    def stop(self) -> None:
        self._stop.set()
        self.transport.stop()
        self.state_server.stop()

    def envelope(self, msg_type: str, **fields: Any) -> Dict[str, Any]:
        msg = {"type": msg_type, "sender": self.id, "epoch": self.epoch,
               "port": self.transport.port, "tcp": self.state_server.port,
               "dispatcher": self.dispatcher}
        msg.update(fields)
        return msg

    # ================================================================== discovery / join
    def _join(self) -> None:
        """HELLO x3. Nobody answers within DISCOVERY_WAIT -> alone -> dispatcher.
        Otherwise fetch the current state from the dispatcher before going live."""
        for _ in range(config.HELLO_REPEAT):
            with self._lock:
                self.transport.multicast(self.envelope(M.HELLO, joining=True))
            time.sleep(config.HELLO_REPEAT_GAP)

        if self._got_reply.wait(config.DISCOVERY_WAIT):
            time.sleep(0.3)                # let the rest of the fleet answer too
            with self._lock:
                log.info("joined fleet: members=%s dispatcher=%s epoch=%s",
                         [short(m) for m in self.view.ids()], short(self.dispatcher), self.epoch)
                self._start_fetch()
        else:
            with self._lock:
                if self.dispatcher is None:
                    log.info("no reply within %.0fs -> I am alone", config.DISCOVERY_WAIT)
                    self.become_dispatcher()
                else:
                    self._start_fetch()

    def _start_fetch(self) -> None:
        if not self._fetching and not self.order.live:
            self._fetching = True
            threading.Thread(target=self._fetch_loop, name="fetch", daemon=True).start()

    def _fetch_loop(self) -> None:
        """Keep asking the current dispatcher for a snapshot (TCP) until we are live."""
        try:
            while not self._stop.is_set():
                with self._lock:
                    if self.order.live:
                        return
                    d = self.dispatcher
                    addr = self.view.tcp_addr(d) if d is not None and d != self.id else None
                if addr:
                    snap = fetch_state(addr)
                    if snap:
                        with self._lock:
                            if self._install(snap):
                                return
                self._stop.wait(1.0)
        finally:
            with self._lock:
                self._fetching = False

    def _install(self, snap: Dict[str, Any]) -> bool:
        if self.order.live:
            return True
        if int(snap["dispatcher"]) != self.dispatcher:
            return False                   # dispatcher changed meanwhile; ask again
        ops, drained = self.order.install(snap["log"], int(snap["last_seq"]))
        now = time.monotonic()
        self.board = TaskBoard()
        for op in ops:
            self.board.apply(op, now)
        log.info("STATE received over TCP: %d ops, %d tasks, last_seq=%d -> now LIVE",
                 len(ops), len(self.board.tasks), self.order.last_delivered)
        for seq, op in drained:
            self._deliver(seq, op)
        self._after_live()
        return True

    def _after_live(self) -> None:
        for t in self.board.open_tasks():
            self._bid(t)
        if self.dispatcher is not None and not self.is_dispatcher and self.id > self.dispatcher:
            log.warning("my ID is higher than dispatcher %s -> election", short(self.dispatcher))
            self.election.start()

    def _resync(self, reason: str) -> None:
        log.warning("%s -> dropping my state and fetching it again", reason)
        self.order.reset()
        self.board = TaskBoard()
        self._start_fetch()

    # ================================================================== dispatcher role
    def become_dispatcher(self) -> None:
        """Called when we win an election (or are alone). New epoch, then sync:
        every robot reports its last delivered seq + ops we miss (COORDINATOR/SYNC_REPLY)."""
        self.election.cancel()
        self.epoch = self.max_epoch_seen + 1
        self.max_epoch_seen = self.epoch
        self.dispatcher = self.id
        self.order.clear_holdback()
        members = self.view.ids()
        log.warning("I am the DISPATCHER now (epoch %d), syncing with %d robot(s)",
                    self.epoch, len(members))
        self.syncing = True
        self._sync_replies = {}
        self._sync_expected = set(members)
        self._sync_deadline = time.monotonic() + config.SYNC_TIMEOUT
        for m in members:
            addr = self.view.addr_of(m)
            if addr:
                self.reliable.send(self.envelope(M.COORDINATOR,
                                                 last_seq=self.order.last_delivered), addr, m)
        if not members:
            self._finish_sync()

    def _finish_sync(self) -> None:
        if not self.syncing:
            return
        self.order.live = True
        # 1. Deliver every op some surviving robot delivered and we did not.
        merged: Dict[int, Any] = {}
        for r in self._sync_replies.values():
            for seq, op in r.get("ops", []):
                merged[int(seq)] = op
        recovered = 0
        for seq in sorted(merged):
            for s, op in self.order.receive(seq, merged[seq]):
                self._deliver(s, op)
                recovered += 1
        self.order.clear_holdback()
        # 2. Re-send what the most-behind robot misses; NACKs fix anything else.
        last = self.order.last_delivered
        lows = [int(r.get("last", 0)) for r in self._sync_replies.values()]
        start = min(lows, default=last) + 1
        for seq in range(start, last + 1):
            if seq in self.order.log:
                self.transport.multicast(self.envelope(M.ORDERED, seq=seq, op=self.order.log[seq]))
        self.syncing = False
        log.warning("SYNC done: %d/%d replies, recovered %d op(s), re-sent %d op(s), "
                    "continuing at seq %d", len(self._sync_replies), len(self._sync_expected),
                    recovered, max(0, last - start + 1), last + 1)
        self._send_heartbeat()
        self._after_live()

    def _send_heartbeat(self) -> None:
        self._last_hb_sent = time.monotonic()
        self.transport.multicast(self.envelope(M.HEARTBEAT, last_seq=self.order.last_delivered))

    def _snapshot_for_joiner(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not (self.is_dispatcher and self.order.live and not self.syncing):
                return None
            return {"type": M.STATE_TRANSFER, "epoch": self.epoch, "dispatcher": self.id,
                    "last_seq": self.order.last_delivered, "log": self.order.entries_after(0)}

    # ================================================================== epochs
    def _adopt(self, d: int, e: int) -> None:
        if d == self.dispatcher and e == self.epoch:
            return
        was_dispatcher = self.is_dispatcher
        if e > self.epoch:
            self.order.clear_holdback()    # old-epoch early ops may be renumbered
        self.epoch = e
        self.max_epoch_seen = max(self.max_epoch_seen, e)
        self.dispatcher = d
        self._last_hb = time.monotonic()
        self.election.cancel()
        if was_dispatcher and d != self.id:
            self.syncing = False
            log.warning("stepping down: %s is dispatcher (epoch %d)", short(d), e)
        else:
            log.info("dispatcher is %s (epoch %d)", short(d), e)

    def _from_dispatcher(self, msg: Dict[str, Any]) -> bool:
        """Epoch check for HEARTBEAT / ORDERED / COORDINATOR. True = accept the message."""
        s, e = int(msg["sender"]), int(msg.get("epoch", 0))
        if e < self.epoch:
            return False                   # old dispatcher: ignore, it will see our epoch
        if e > self.epoch or self.dispatcher is None:
            self._adopt(s, e)
            return True
        if s == self.dispatcher:
            return True
        if s > self.dispatcher:            # two dispatchers in one epoch: higher ID wins
            was_dispatcher = self.is_dispatcher
            self._adopt(s, e)
            if was_dispatcher:
                self._resync(f"two dispatchers in epoch {e}")
            return True
        return False

    def _hint(self, msg: Dict[str, Any]) -> None:
        """Second-hand dispatcher info from HELLO / HELLO_REPLY."""
        d, e = msg.get("dispatcher"), int(msg.get("epoch", 0))
        if d is None or int(d) == self.id:
            return
        if e > self.epoch or (self.dispatcher is None and e >= self.epoch):
            self._adopt(int(d), e)

    # ================================================================== message handling
    def _on_message(self, msg: Dict[str, Any], addr: Addr) -> None:
        sender = int(msg["sender"])
        if sender == self.id:
            return                         # our own multicast looped back
        if not self.reliable.accept(msg, addr):
            return
        with self._lock:
            self.view.touch(sender, addr, msg.get("tcp"))
            self.max_epoch_seen = max(self.max_epoch_seen, int(msg.get("epoch", 0)))
            handler = self._handlers.get(msg["type"])
            if handler:
                handler(msg, addr)

    def _on_hello(self, msg: Dict[str, Any], addr: Addr) -> None:
        if msg.get("joining"):
            self.transport.unicast(self.envelope(M.HELLO_REPLY), addr)
        self._hint(msg)

    def _on_hello_reply(self, msg: Dict[str, Any], addr: Addr) -> None:
        self._hint(msg)
        self._got_reply.set()

    def _on_heartbeat(self, msg: Dict[str, Any], addr: Addr) -> None:
        if self._from_dispatcher(msg):
            self._last_hb = time.monotonic()
            self.order.known_max = max(self.order.known_max, int(msg.get("last_seq", 0)))

    def _on_election(self, msg: Dict[str, Any], addr: Addr) -> None:
        self.election.on_election(msg, addr)

    def _on_answer(self, msg: Dict[str, Any], addr: Addr) -> None:
        self.election.on_answer(msg)

    def _on_coordinator(self, msg: Dict[str, Any], addr: Addr) -> None:
        if not self._from_dispatcher(msg):
            return
        log.info("COORDINATOR %s (epoch %d): reporting last_seq=%d",
                 short(int(msg["sender"])), self.epoch, self.order.last_delivered)
        ops = self.order.entries_after(int(msg.get("last_seq", 0))) if self.order.live else []
        self.reliable.send(self.envelope(M.SYNC_REPLY, last=self.order.last_delivered, ops=ops),
                           addr, int(msg["sender"]))

    def _on_sync_reply(self, msg: Dict[str, Any], addr: Addr) -> None:
        if self.is_dispatcher and self.syncing and int(msg.get("epoch", 0)) == self.epoch:
            self._sync_replies[int(msg["sender"])] = msg
            if self._sync_expected <= set(self._sync_replies):
                self._finish_sync()

    def _on_request(self, msg: Dict[str, Any], addr: Addr) -> None:
        if self.is_dispatcher and self.order.live and not self.syncing:
            self._sequence(msg["op"])

    def _on_ordered(self, msg: Dict[str, Any], addr: Addr) -> None:
        if self._from_dispatcher(msg):
            for seq, op in self.order.receive(int(msg["seq"]), msg["op"]):
                self._deliver(seq, op)

    def _on_nack(self, msg: Dict[str, Any], addr: Addr) -> None:
        if not self.is_dispatcher:
            return
        lo = int(msg["lo"])
        hi = min(int(msg["hi"]), self.order.last_delivered, lo + config.NACK_MAX_BATCH - 1)
        for seq in range(lo, hi + 1):
            if seq in self.order.log:
                self.transport.unicast(self.envelope(M.ORDERED, seq=seq,
                                                     op=self.order.log[seq]), addr)
        log.debug("NACK from %s: re-sent seq %d..%d", short(int(msg["sender"])), lo, hi)

    # ================================================================== total order
    def submit(self, op: Dict[str, Any]) -> None:
        """Ask for op to be ordered. Re-sent every RESUBMIT_INTERVAL until delivered."""
        self.order.pending[op["op_id"]] = op
        self._send_request(op)

    def _send_request(self, op: Dict[str, Any]) -> None:
        if self.is_dispatcher:
            if self.order.live and not self.syncing:
                self._sequence(op)
        elif self.dispatcher is not None:
            addr = self.view.addr_of(self.dispatcher)
            if addr:
                self.transport.unicast(self.envelope(M.REQUEST, op=op), addr)

    def _sequence(self, op: Dict[str, Any]) -> None:
        """Sequencer: give op the next global number, multicast it, deliver it locally."""
        seq = self.order.assign_seq(op)
        if seq is None:
            return                         # duplicate (same op_id already ordered)
        self.transport.multicast(self.envelope(M.ORDERED, seq=seq, op=op))
        for s, o in self.order.receive(seq, op):
            self._deliver(s, o)

    # ================================================================== task allocation
    def _deliver(self, seq: int, op: Dict[str, Any]) -> None:
        res = self.board.apply(op, time.monotonic())
        if res is None:
            return
        kind, t = res
        if kind == "open":
            log.info("[seq %d] task %s '%s' OPEN at (%.0f,%.0f), bidding %.0fs",
                     seq, t["id"], t["name"], t["x"], t["y"], t["duration"])
            self._bid(t)
        elif kind == "bid":
            if op["robot"] == self.id:
                log.info("[seq %d] my bid for %s accepted into the order (cost %.1f)",
                         seq, t["id"], op["cost"])
        elif kind == "late_bid":
            if op["robot"] == self.id:
                log.info("[seq %d] my bid for %s came after CLOSE - not counted",
                         seq, op["task_id"])
        elif kind == "close":
            w = t["winner"]
            if w is None:
                log.info("[seq %d] RESULT task=%s winner=none", seq, t["id"])
            else:
                log.info("[seq %d] RESULT task=%s winner=%s cost=%.1f bids=%d",
                         seq, t["id"], short(w), t["cost"], len(t["bids"]))
            if w == self.id:
                self.scorer.assign(t)
                log.warning("*** I won task %s, moving to (%.0f,%.0f), load=%d",
                            t["id"], self.scorer.x, self.scorer.y, self.scorer.load)

    def _bid(self, t: Dict[str, Any]) -> None:
        if not self.order.live:
            return
        bid_id = f"bid:{t['id']}:{self.id}"     # same ID on resend -> never counted twice
        if bid_id in self.order.delivered_ids or bid_id in self.order.pending:
            return
        self.submit({"kind": "bid", "op_id": bid_id, "bid_id": bid_id, "task_id": t["id"],
                     "robot": self.id, "cost": self.scorer.cost(t)})

    def post_task(self, name: str, x: float, y: float, duration: float) -> str:
        with self._lock:
            if not self.order.live:
                raise RuntimeError("not live yet (still joining)")
            self._task_counter += 1
            task_id = f"{short(self.id)}-{self._task_counter}"
            self.submit({"kind": "open", "op_id": f"open:{task_id}", "task_id": task_id,
                         "name": name, "x": x, "y": y, "duration": duration, "by": self.id})
            return task_id

    # ================================================================== timers
    def _tick_loop(self) -> None:
        while not self._stop.wait(config.TICK):
            with self._lock:
                try:
                    self._tick()
                except Exception:
                    log.exception("tick failed")

    def _tick(self) -> None:
        now = time.monotonic()
        for dest in self.reliable.tick():
            self._on_give_up(dest)
        self.election.tick()

        if now - self._last_hello >= config.HELLO_INTERVAL:
            self._last_hello = now
            self.transport.multicast(self.envelope(M.HELLO, joining=False))
            self.view.expire()

        if self.is_dispatcher:
            if self.syncing:
                if now >= self._sync_deadline:
                    self._finish_sync()
            else:
                if now - self._last_hb_sent >= config.HEARTBEAT_INTERVAL:
                    self._send_heartbeat()
                for t in self.board.open_tasks():          # bidding time over -> CLOSE
                    op_id = "close:" + t["id"]
                    if now >= t["deadline"] and op_id not in self.order.pending:
                        self.submit({"kind": "close", "op_id": op_id, "task_id": t["id"]})
        else:
            if (not self.election.running and
                    now - self._last_hb > config.HEARTBEAT_INTERVAL * config.HEARTBEAT_MISSES):
                self._suspect_dispatcher()
            if (self.order.live and self.dispatcher is not None
                    and now - self._last_nack >= config.NACK_INTERVAL):
                gap = self.order.missing_range()
                addr = self.view.addr_of(self.dispatcher)
                if gap and addr:
                    self._last_nack = now
                    log.debug("gap detected, NACK seq %d..%d", *gap)
                    self.transport.unicast(self.envelope(M.NACK, lo=gap[0], hi=gap[1]), addr)

        if self.order.pending and now - self._last_resubmit >= config.RESUBMIT_INTERVAL:
            self._last_resubmit = now
            for op in list(self.order.pending.values()):
                self._send_request(op)

    def _suspect_dispatcher(self) -> None:
        d = self.dispatcher
        if d is None:
            log.warning("no dispatcher heard for %.0fs -> election",
                        config.HEARTBEAT_INTERVAL * config.HEARTBEAT_MISSES)
        else:
            log.warning("dispatcher %s missed %d heartbeats -> assume crashed, election",
                        short(d), config.HEARTBEAT_MISSES)
            self.view.remove(d)
        self.dispatcher = None
        self._last_hb = time.monotonic()
        self.election.start()

    def _on_give_up(self, dest: int) -> None:
        log.warning("no ACK from %s after %d tries -> suspect crashed",
                    short(dest), config.MAX_RETRIES)
        self.view.remove(dest)
        if self.syncing:
            self._sync_expected.discard(dest)
            if self._sync_expected <= set(self._sync_replies):
                self._finish_sync()

    # ================================================================== CLI views
    def status(self) -> str:
        with self._lock:
            role = "DISPATCHER" if self.is_dispatcher else "bidder"
            return (f"id={short(self.id)} role={role} dispatcher={short(self.dispatcher)} "
                    f"epoch={self.epoch} live={self.order.live} last_seq={self.order.last_delivered} "
                    f"pending={len(self.order.pending)} pos=({self.scorer.x:.0f},{self.scorer.y:.0f}) "
                    f"load={self.scorer.load} election={'yes' if self.election.running else 'no'}")

    def task_lines(self) -> List[str]:
        with self._lock:
            lines = []
            for t in self.board.tasks.values():
                extra = (f"winner={short(t['winner'])} cost={t['cost']}" if t["winner"] is not None
                         else f"{len(t['bids'])} bid(s)")
                lines.append(f"  {t['id']:<12} {t['name']:<12} ({t['x']:.0f},{t['y']:.0f}) "
                             f"{t['status']:<10} {extra}")
            return lines
