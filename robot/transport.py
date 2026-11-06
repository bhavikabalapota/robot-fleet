"""UDP transport: one multicast receive socket and one unicast socket.

All outgoing traffic (multicast and unicast) leaves from the unicast socket, so
a receiver can reply to the datagram's source address directly.
"""
from __future__ import annotations

import logging
import random
import socket
import struct
import threading
from typing import Any, Callable, Dict, Tuple

from . import config, messages

log = logging.getLogger("robot.transport")

Addr = Tuple[str, int]
Handler = Callable[[Dict[str, Any], Addr], None]


class Transport:
    def __init__(self, handler: Handler, loss: float = 0.0):
        self._handler = handler
        self.loss = loss            # probability of dropping an incoming datagram (testing)
        self._running = False

        # Unicast socket: ephemeral port, also used to send multicast.
        self.ucast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ucast.bind(("0.0.0.0", 0))
        self.ucast.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, config.MCAST_TTL)
        self.ucast.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        self.port = self.ucast.getsockname()[1]

        # Multicast socket: several robots on one host share the group port.
        self.mcast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.mcast.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            self.mcast.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.mcast.bind(("", config.MCAST_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(config.MCAST_GROUP), socket.INADDR_ANY)
        self.mcast.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def start(self) -> None:
        self._running = True
        for sock, name in ((self.ucast, "ucast-rx"), (self.mcast, "mcast-rx")):
            threading.Thread(target=self._recv_loop, args=(sock,), name=name, daemon=True).start()

    def stop(self) -> None:
        self._running = False
        for sock in (self.ucast, self.mcast):
            try:
                sock.close()
            except OSError:
                pass

    def multicast(self, msg: Dict[str, Any]) -> None:
        self._send(msg, (config.MCAST_GROUP, config.MCAST_PORT))

    def unicast(self, msg: Dict[str, Any], addr: Addr) -> None:
        self._send(msg, addr)

    def _send(self, msg: Dict[str, Any], addr: Addr) -> None:
        try:
            self.ucast.sendto(messages.encode(msg), addr)
        except OSError as e:
            if self._running:
                log.warning("send to %s failed: %s", addr, e)

    def _recv_loop(self, sock: socket.socket) -> None:
        while self._running:
            try:
                data, src = sock.recvfrom(config.BUFFER_SIZE)
            except OSError:
                break
            if self.loss and random.random() < self.loss:
                continue            # simulated packet loss
            try:
                msg = messages.decode(data)
            except (ValueError, UnicodeDecodeError):
                log.debug("dropping malformed datagram from %s", src)
                continue
            # Reply address = source IP + the unicast port the sender announced.
            reply_addr = (src[0], int(msg.get("port", src[1])))
            try:
                self._handler(msg, reply_addr)
            except Exception:
                log.exception("handler error for %s", msg.get("type"))
