"""Automated test scenarios: start real robot processes, drive them through the CLI,
and check that all surviving robots agree on every task result.

    python3 scripts/scenarios.py            # run all
    python3 scripts/scenarios.py failover   # run one
Logs are written to logs/<scenario>/robot-<id>.log
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from typing import Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT = re.compile(r"RESULT task=(\S+) winner=(\S+)")


class Bot:
    def __init__(self, scenario: str, rid: int, *extra: str):
        self.id = rid
        d = os.path.join(ROOT, "logs", scenario)
        os.makedirs(d, exist_ok=True)
        self.path = os.path.join(d, f"robot-{rid}.log")
        self.out = open(self.path, "w")
        self.p = subprocess.Popen([sys.executable, "-u", "-m", "robot", "--id", str(rid), *extra],
                                  cwd=ROOT, stdin=subprocess.PIPE, stdout=self.out,
                                  stderr=subprocess.STDOUT, text=True)

    def cmd(self, line: str) -> None:
        self.p.stdin.write(line + "\n")
        self.p.stdin.flush()

    def kill(self) -> None:
        self.p.kill()        # crash-stop: no goodbye message
        self.p.wait()

    def log(self) -> str:
        with open(self.path) as f:
            return f.read()

    def results(self) -> Dict[str, str]:
        return dict(RESULT.findall(self.log()))


def check(name: str, bots: List[Bot], expected_tasks: int) -> bool:
    res = {b.id: b.results() for b in bots}
    for b in bots:
        print(f"    robot {b.id}: {res[b.id]}")
    ref = res[bots[0].id]
    ok = len(ref) == expected_tasks and all(r == ref for r in res.values())
    print(f"  {'PASS' if ok else 'FAIL'}: {name} "
          f"({len(ref)}/{expected_tasks} tasks, {'consistent' if ok else 'MISMATCH'})")
    return ok


def contains(bot: Bot, text: str) -> bool:
    found = text in bot.log()
    print(f"  {'PASS' if found else 'FAIL'}: robot {bot.id} log contains '{text}'")
    return found


def cleanup(bots: List[Bot]) -> None:
    for b in bots:
        if b.p.poll() is None:
            b.kill()


# ---------------------------------------------------------------------------- scenarios
def basic() -> bool:
    """3 robots, two tasks posted from different robots; all agree on the winners."""
    s = "basic"
    bots = [Bot(s, 10, "--pos", "0", "0")]
    time.sleep(3.5)
    bots += [Bot(s, 20, "--pos", "50", "50"), Bot(s, 30, "--pos", "100", "100")]
    time.sleep(6)
    bots[0].cmd("post near-origin 5 5 4")
    bots[1].cmd("post far-corner 95 95 4")
    time.sleep(8)
    try:
        return (check("basic allocation", bots, 2)
                and contains(bots[0], "winner=10")          # near-origin -> robot at (0,0)
                and contains(bots[0], "winner=30"))         # far-corner -> robot at (100,100)
    finally:
        cleanup(bots)


def loss() -> bool:
    """30% simulated packet loss on every robot; NACK/retransmit keep everyone consistent."""
    s = "loss"
    bots = [Bot(s, 10, "--loss", "0.3")]
    time.sleep(3.5)
    bots += [Bot(s, 20, "--loss", "0.3"), Bot(s, 30, "--loss", "0.3")]
    time.sleep(8)
    for i in range(4):
        bots[i % 3].cmd(f"post t{i} {10 + 20 * i} {80 - 15 * i} 4")
        time.sleep(0.5)
    time.sleep(14)
    try:
        return check("total order under 30% loss", bots, 4)
    finally:
        cleanup(bots)


def failover() -> bool:
    """Dispatcher crashes in the middle of bidding; a new one is elected and finishes it."""
    s = "failover"
    bots = [Bot(s, 30)]
    time.sleep(3.5)
    bots += [Bot(s, 10), Bot(s, 20)]
    time.sleep(6)
    bots[1].cmd("post before-crash 20 20 3")
    time.sleep(5)
    bots[1].cmd("post during-crash 40 40 10")
    time.sleep(2)
    print("  killing dispatcher 30")
    bots[0].kill()
    time.sleep(22)
    alive = bots[1:]
    try:
        return (check("allocation survives dispatcher crash", alive, 2)
                and contains(bots[2], "I am the DISPATCHER now")
                and contains(bots[1], "dispatcher is 20"))
    finally:
        cleanup(bots)


def join() -> bool:
    """A robot joins late, gets the state over TCP; a higher-ID joiner triggers an election."""
    s = "join"
    bots = [Bot(s, 20)]
    time.sleep(3.5)
    bots.append(Bot(s, 10))
    time.sleep(5)
    bots[0].cmd("post early 30 30 3")
    time.sleep(6)
    bots.append(Bot(s, 5))       # lower ID: just joins
    time.sleep(6)
    bots.append(Bot(s, 40))      # higher ID: joins, then wins an election
    time.sleep(12)
    bots[2].cmd("post late 60 60 3")
    time.sleep(8)
    try:
        r5 = bots[2].results()
        ok = (contains(bots[2], "STATE received over TCP")
              and contains(bots[3], "I am the DISPATCHER now")
              and contains(bots[0], "stepping down"))
        # late joiners did not deliver 'early' live; compare only the 'late' task
        late = {b.id: {k: v for k, v in b.results().items() if k.startswith("5-")} for b in bots}
        same = len(late[5]) == 1 and all(v == late[5] for v in late.values())
        print(f"  {'PASS' if same else 'FAIL'}: all 4 robots agree on the task posted after joins "
              f"{late}")
        print(f"    robot 5 RESULTs: {r5}")
        return ok and same
    finally:
        cleanup(bots)


SCENARIOS = {"basic": basic, "loss": loss, "failover": failover, "join": join}

if __name__ == "__main__":
    names = sys.argv[1:] or list(SCENARIOS)
    results = {}
    for n in names:
        print(f"== {n}: {SCENARIOS[n].__doc__}")
        results[n] = SCENARIOS[n]()
        time.sleep(2)
    print("\nSUMMARY: " + ", ".join(f"{n}={'PASS' if ok else 'FAIL'}" for n, ok in results.items()))
    sys.exit(0 if all(results.values()) else 1)
