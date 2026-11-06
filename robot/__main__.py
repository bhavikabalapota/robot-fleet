"""Entry point: python -m robot [--id N] [--loss 0.2] [--pos X Y] [--log-level DEBUG]"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time

from . import config
from .node import Robot
from .util import short

HELP = """commands:
  post NAME [X Y] [SECONDS]  post a new task (random position, default bidding time)
  tasks                      show the replicated task board
  status                     role, dispatcher, epoch, sequence number, position
  view                       membership view
  help | quit"""


def main() -> None:
    p = argparse.ArgumentParser(description="Decentralized robot task allocation")
    p.add_argument("--id", type=int, help="robot ID (default: random UUID)")
    p.add_argument("--loss", type=float, default=0.0, help="simulated incoming packet loss 0..1")
    p.add_argument("--pos", type=float, nargs=2, metavar=("X", "Y"), help="start position")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()

    robot = Robot(args.id, loss=args.loss, pos=tuple(args.pos) if args.pos else None)
    logging.basicConfig(
        level=args.log_level.upper(), stream=sys.stdout,
        format=f"%(asctime)s.%(msecs)03d [{short(robot.id)}] %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    print(HELP, flush=True)
    robot.start()

    try:
        for line in sys.stdin:
            handle(robot, line.split())
            sys.stdout.flush()
        while True:                      # stdin closed (started in background): keep running
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        robot.stop()


def handle(robot: Robot, parts: list) -> None:
    if not parts:
        return
    cmd = parts[0].lower()
    if cmd == "status":
        print(robot.status())
    elif cmd == "view":
        members = robot.view.snapshot()
        print(f"{len(members)} other member(s):")
        for m in members:
            tag = " (dispatcher)" if m.robot_id == robot.dispatcher else ""
            print(f"  {short(m.robot_id)} @ {m.addr[0]}:{m.addr[1]}{tag}")
    elif cmd == "tasks":
        lines = robot.task_lines()
        print("\n".join(lines) if lines else "no tasks")
    elif cmd == "post":
        try:
            name = parts[1] if len(parts) > 1 else "task"
            x, y = ((float(parts[2]), float(parts[3])) if len(parts) > 3
                    else (random.uniform(0, 100), random.uniform(0, 100)))
            dur = float(parts[4]) if len(parts) > 4 else config.DEFAULT_BID_TIME
            print(f"posted {robot.post_task(name, x, y, dur)}")
        except (ValueError, RuntimeError) as e:
            print(f"cannot post: {e}")
    elif cmd in ("quit", "exit"):
        raise SystemExit
    elif cmd in ("help", "?"):
        print(HELP)
    else:
        print(f"unknown command: {cmd}")


if __name__ == "__main__":
    main()
