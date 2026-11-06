"""Task state replica and bid scorer.

The task board is changed ONLY by delivering ordered operations, so every robot
that delivered the same sequence has the same board:
    open  {task_id, name, x, y, duration, by}
    bid   {bid_id, task_id, robot, cost}
    close {task_id}
Only bids ordered before the CLOSE count. Lowest cost wins; ties -> lowest robot ID.
"""
from __future__ import annotations

import math
import random
from typing import Any, Dict, List, Optional, Tuple

from . import config

Task = Dict[str, Any]


class TaskBoard:
    def __init__(self) -> None:
        self.tasks: Dict[str, Task] = {}

    def apply(self, op: Dict[str, Any], now: float) -> Optional[Tuple[str, Any]]:
        kind = op["kind"]
        if kind == "open":
            if op["task_id"] in self.tasks:
                return None
            t = {"id": op["task_id"], "name": op["name"], "x": op["x"], "y": op["y"],
                 "duration": op["duration"], "by": op["by"], "status": "open",
                 "bids": {}, "winner": None, "cost": None,
                 # Local deadline: each robot measures the bidding time from its own
                 # delivery of OPEN, so a new dispatcher can finish the remaining time.
                 "deadline": now + op["duration"]}
            self.tasks[t["id"]] = t
            return "open", t
        if kind == "bid":
            t = self.tasks.get(op["task_id"])
            if t is None or t["status"] != "open":
                return "late_bid", op
            t["bids"][op["bid_id"]] = {"robot": op["robot"], "cost": op["cost"]}
            return "bid", t
        if kind == "close":
            t = self.tasks.get(op["task_id"])
            if t is None or t["status"] != "open":
                return None
            if t["bids"]:
                best = min(t["bids"].values(), key=lambda b: (b["cost"], b["robot"]))
                t["winner"], t["cost"], t["status"] = best["robot"], best["cost"], "assigned"
            else:
                t["status"] = "unassigned"
            return "close", t
        return None

    def open_tasks(self) -> List[Task]:
        return [t for t in self.tasks.values() if t["status"] == "open"]


class Scorer:
    """Cost of doing a task = distance to it + penalty per task already assigned."""

    def __init__(self, pos: Optional[Tuple[float, float]] = None):
        self.x, self.y = pos if pos else (random.uniform(0, 100), random.uniform(0, 100))
        self.load = 0

    def cost(self, t: Task) -> float:
        return round(math.hypot(t["x"] - self.x, t["y"] - self.y)
                     + config.LOAD_PENALTY * self.load, 2)

    def assign(self, t: Task) -> None:
        self.load += 1
        self.x, self.y = t["x"], t["y"]
