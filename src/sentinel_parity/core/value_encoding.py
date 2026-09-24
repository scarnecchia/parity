# pattern: Functional Core
"""Injective canonical equality keys and typed JSON envelopes."""

from __future__ import annotations

import base64
import math
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any


def numeric_key(value: int | float | Decimal) -> str:
    if isinstance(value, bool):
        raise TypeError("boolean is not a numeric cell")
    if isinstance(value, float):
        if math.isnan(value):
            return "f:nan"
        if math.isinf(value):
            return "f:+inf" if value > 0 else "f:-inf"
        ratio = Fraction(*value.as_integer_ratio())
    elif isinstance(value, Decimal):
        if value.is_nan():
            return "f:nan"
        if value.is_infinite():
            return "f:+inf" if value > 0 else "f:-inf"
        ratio = Fraction(value)
    else:
        ratio = Fraction(value, 1)
    return f"n:{ratio.numerator}/{ratio.denominator}"


def temporal_ns_key(epoch_ns: int, timezone_aware: bool) -> str:
    prefix = "dt-aware-ns:" if timezone_aware else "dt-naive-ns:"
    return prefix + str(epoch_ns)


def temporal_ns_iso(epoch_ns: int, timezone_aware: bool) -> str:
    seconds, nanoseconds = divmod(epoch_ns, 1_000_000_000)
    instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        seconds=seconds, microseconds=nanoseconds // 1000
    )
    suffix = f"{nanoseconds:09d}"
    base = instant.strftime("%Y-%m-%dT%H:%M:%S")
    zone = "+00:00" if timezone_aware else ""
    return f"{base}.{suffix}{zone}"


def canonical_key(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "b:1" if value else "b:0"
    if isinstance(value, (int, float, Decimal)):
        return numeric_key(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return "dt-naive:" + value.isoformat(timespec="microseconds")
        return "dt-aware:" + value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return "date:" + value.isoformat()
    if isinstance(value, str):
        return "s:" + value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "y:" + base64.b64encode(bytes(value)).decode("ascii")
    raise TypeError(f"unsupported logical value type: {type(value).__name__}")


def typed_value(value: Any, logical_type: str | None = None) -> dict[str, object] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return {"type": "boolean", "value": value}
    if isinstance(value, (int, float, Decimal)):
        return {
            "type": logical_type or type(value).__name__,
            "value": str(value),
            "canonical": numeric_key(value),
        }
    if isinstance(value, datetime):
        return {
            "type": logical_type or "timestamp",
            "value": value.isoformat(timespec="microseconds"),
        }
    if isinstance(value, date):
        return {"type": logical_type or "date", "value": value.isoformat()}
    if isinstance(value, str):
        return {"type": logical_type or "string", "value": value}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "type": logical_type or "binary",
            "value": base64.b64encode(bytes(value)).decode("ascii"),
        }
    raise TypeError(f"unsupported logical value type: {type(value).__name__}")
