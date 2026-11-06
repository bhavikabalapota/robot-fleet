from __future__ import annotations

from typing import Optional


def short(robot_id: Optional[int]) -> str:
    """Readable form of a robot ID: small (manual) IDs as-is, UUID IDs as 8 hex chars."""
    if robot_id is None:
        return "-"
    if robot_id < 1_000_000:
        return str(robot_id)
    return f"{robot_id:032x}"[:8]
