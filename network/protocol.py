"""
Wire protocol — MessagePack over TCP.

Frame format:
  [4 bytes big-endian uint32 = body length][body = msgpack bytes]

Command structures (Python dicts encoded as msgpack):

  Request:
    { "cmd": "SET", "key": "user:1", "value": b"Anurag", "ttl": 3600.0 }
    { "cmd": "GET", "key": "user:1" }
    { "cmd": "DEL", "key": "user:1" }
    { "cmd": "PING" }
    { "cmd": "METRICS" }

  Response:
    { "ok": True, "value": b"Anurag" }   ← GET hit
    { "ok": True, "value": None }         ← GET miss
    { "ok": True }                        ← SET / DEL / PING
    { "ok": False, "error": "reason" }    ← any error
    { "ok": True, "metrics": {...} }      ← METRICS
"""

from __future__ import annotations

import struct

import msgpack

import asyncio

_LENGTH_FMT = ">I"
_LENGTH_SIZE = struct.calcsize(_LENGTH_FMT)

VALID_COMMANDS = {"SET", "GET", "DEL", "PING", "METRICS", "REPLICATE", "MIGRATE"}


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def encode(obj: dict) -> bytes:
    """Encode a command/response dict to a length-prefixed msgpack frame."""
    body = msgpack.packb(obj, use_bin_type=True)
    return struct.pack(_LENGTH_FMT, len(body)) + body


def encode_ok(**kwargs) -> bytes:
    return encode({"ok": True, **kwargs})


def encode_error(reason: str) -> bytes:
    return encode({"ok": False, "error": reason})


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


async def read_message(reader: "asyncio.StreamReader") -> dict:
    """
    Read one complete framed message from an asyncio StreamReader.
    Raises asyncio.IncompleteReadError if the connection closes mid-frame.
    """
    header = await reader.readexactly(_LENGTH_SIZE)
    (length,) = struct.unpack(_LENGTH_FMT, header)
    body = await reader.readexactly(length)
    return msgpack.unpackb(body, raw=False, strict_map_key=False)


def decode(data: bytes) -> dict:
    """Decode a raw msgpack body (no length prefix)."""
    return msgpack.unpackb(data, raw=False, strict_map_key=False)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_command(msg: dict) -> tuple[bool, str]:
    """
    Validate an incoming command dict.
    Returns (is_valid, error_reason).
    """
    msg.setdefault("forwarded", False)
    msg.setdefault("origin", None)
    cmd = msg.get("cmd")
    if not cmd:
        return False, "Missing 'cmd' field"
    if cmd not in VALID_COMMANDS:
        return False, f"Unknown command: {cmd!r}"
    if cmd in ("SET", "GET", "DEL", "REPLICATE") and not msg.get("key"):
        return False, f"{cmd} requires a 'key' field"
    if cmd == "SET" and "value" not in msg:
        return False, "SET requires a 'value' field"
    return True, ""
