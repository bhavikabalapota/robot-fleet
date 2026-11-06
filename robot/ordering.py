"""Total-order delivery: hold-back queue, delivered log, gap detection.

The dispatcher (sequencer) assigns global sequence numbers; every robot delivers
operations strictly in sequence order. Every robot keeps the full delivered log,
so any robot can serve NACKs / state after it becomes dispatcher.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

Op = Dict[str, Any]


class TotalOrder:
    def __init__(self) -> None:
        self.pending: Dict[str, Op] = {}   # own ops submitted but not yet delivered
        self.reset()

    def reset(self) -> None:
        self.log: Dict[int, Op] = {}       # seq -> op (delivered, contiguous 1..last_delivered)
        self.last_delivered = 0
        self.holdback: Dict[int, Op] = {}  # seq -> op that arrived too early
        self.delivered_ids: set = set()    # op_ids already delivered (bid IDs etc.)
        self.known_max = 0                 # highest seq we know exists (ORDERED / heartbeat)
        self.live = False                  # False until we have the current state

    # -------------------------------------------------------------- sequencer side
    def assign_seq(self, op: Op) -> Optional[int]:
        """Next sequence number for op, or None if op was already ordered (duplicate)."""
        if op["op_id"] in self.delivered_ids:
            return None
        return self.last_delivered + 1

    # -------------------------------------------------------------- receiver side
    def receive(self, seq: int, op: Op) -> List[Tuple[int, Op]]:
        """Accept an ordered op. Returns the ops that are now deliverable, in order."""
        if seq <= self.last_delivered:
            return []
        self.holdback[seq] = op
        self.known_max = max(self.known_max, seq)
        return self._drain() if self.live else []

    def _drain(self) -> List[Tuple[int, Op]]:
        out = []
        while self.last_delivered + 1 in self.holdback:
            seq = self.last_delivered + 1
            op = self.holdback.pop(seq)
            self.last_delivered = seq
            self.log[seq] = op
            if op["op_id"] in self.delivered_ids:
                continue                   # same op ordered twice: apply only once
            self.delivered_ids.add(op["op_id"])
            self.pending.pop(op["op_id"], None)
            out.append((seq, op))
        return out

    def install(self, entries: List[list], last_seq: int) -> Tuple[List[Op], List[Tuple[int, Op]]]:
        """Install a snapshot (list of [seq, op]). Returns (unique snapshot ops in order,
        ops from the hold-back queue that became deliverable afterwards)."""
        self.log, self.delivered_ids, unique = {}, set(), []
        for seq, op in sorted(entries, key=lambda e: e[0]):
            self.log[int(seq)] = op
            if op["op_id"] not in self.delivered_ids:
                self.delivered_ids.add(op["op_id"])
                self.pending.pop(op["op_id"], None)
                unique.append(op)
        self.last_delivered = last_seq
        self.known_max = max(self.known_max, last_seq)
        self.holdback = {s: o for s, o in self.holdback.items() if s > last_seq}
        self.live = True
        return unique, self._drain()

    def missing_range(self) -> Optional[Tuple[int, int]]:
        hi = max([self.known_max, *self.holdback.keys()])
        return (self.last_delivered + 1, hi) if hi > self.last_delivered else None

    def entries_after(self, seq: int) -> List[list]:
        return [[s, self.log[s]] for s in range(seq + 1, self.last_delivered + 1) if s in self.log]

    def clear_holdback(self) -> None:
        """Called on an epoch change: early ops of the old epoch may be renumbered."""
        self.holdback.clear()
        self.known_max = self.last_delivered
