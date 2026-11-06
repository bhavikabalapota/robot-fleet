"""Wire format: one JSON object per UDP datagram.

Every message carries:
    type        message type (constants below)
    sender      robot ID of the sender
    epoch       sender's current epoch (dispatcher messages with an older epoch are ignored)
    port        sender's unicast UDP port (IP is taken from the datagram source)
    tcp         sender's TCP port for state transfer
    dispatcher  who the sender thinks the dispatcher is
Reliable unicast messages additionally carry `mid` (per-sender message ID).
"""
from __future__ import annotations

import json
from typing import Any, Dict

# Discovery (multicast / unicast)
HELLO = "HELLO"
HELLO_REPLY = "HELLO_REPLY"

# Reliable unicast
ACK = "ACK"

# Failure detection + Bully election
HEARTBEAT = "HEARTBEAT"          # dispatcher -> all (multicast), carries last_seq
ELECTION = "ELECTION"
ANSWER = "ANSWER"
COORDINATOR = "COORDINATOR"      # new dispatcher -> each robot; also asks for SYNC_REPLY
SYNC_REPLY = "SYNC_REPLY"        # robot -> new dispatcher: last delivered seq + missing ops

# Total-order multicast
REQUEST = "REQUEST"              # robot -> dispatcher: please order this op
ORDERED = "ORDERED"              # dispatcher -> all: op with global sequence number
NACK = "NACK"                    # robot -> dispatcher: I miss seq lo..hi

# State transfer (TCP, newline-delimited JSON)
STATE_REQUEST = "STATE_REQUEST"
STATE_TRANSFER = "STATE_TRANSFER"
STATE_DENIED = "STATE_DENIED"


def encode(msg: Dict[str, Any]) -> bytes:
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> Dict[str, Any]:
    msg = json.loads(data.decode("utf-8"))
    if not isinstance(msg, dict) or "type" not in msg or "sender" not in msg:
        raise ValueError("malformed message")
    return msg
