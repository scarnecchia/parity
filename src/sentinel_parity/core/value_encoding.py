# pattern: Functional Core
"""Value-space equality keys and typed display envelopes.

Cells match when their decoded values are equal; declared storage types are
never a factor. 16.0 = 16 while 16.2 != 16, True = 1, naive timestamps are
instants read as UTC, dates equal midnight instants, numeric-looking text
compares as numbers, and every missing form (null, NaN, blank text) is one
value.
"""

from __future__ import annotations

import base64
import math
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any


def numeric_key(value: int | float | Decimal) -> str:
    if isinstance(value, bool):
        value = int(value)
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


_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_AWARE = datetime(1970, 1, 1, tzinfo=UTC)


def _instant_ns(value: datetime) -> int:
    delta = value - _EPOCH_NAIVE if value.tzinfo is None else value.astimezone(UTC) - _EPOCH_AWARE
    return (delta // timedelta(microseconds=1)) * 1000


def temporal_ns_key(epoch_ns: int) -> str:
    return "dt-ns:" + str(epoch_ns)


def temporal_ns_iso(epoch_ns: int, timezone_aware: bool) -> str:
    seconds, nanoseconds = divmod(epoch_ns, 1_000_000_000)
    instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        seconds=seconds, microseconds=nanoseconds // 1000
    )
    suffix = f"{nanoseconds:09d}"
    base = instant.strftime("%Y-%m-%dT%H:%M:%S")
    zone = "+00:00" if timezone_aware else ""
    return f"{base}.{suffix}{zone}"


def _text_key(value: str) -> str:
    text = value.rstrip()
    if not text:
        return "null"
    try:
        number = Decimal(text)
    except InvalidOperation:
        return "s:" + text
    if number.is_nan():
        return "null"
    return numeric_key(number)


def canonical_key(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return numeric_key(int(value))
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and math.isnan(value):
            return "null"
        if isinstance(value, Decimal) and value.is_nan():
            return "null"
        return numeric_key(value)
    if isinstance(value, datetime):
        return temporal_ns_key(_instant_ns(value))
    if isinstance(value, date):
        days = value.toordinal() - _EPOCH_NAIVE.date().toordinal()
        return temporal_ns_key(days * 86_400_000_000_000)
    if isinstance(value, str):
        return _text_key(value)
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
            "canonical": canonical_key(value),
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
