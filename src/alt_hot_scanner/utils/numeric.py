from __future__ import annotations

import math
import numbers
import re

_INTEGER_TEXT = re.compile(r"[+-]?\d+")


def strict_raw_integer(
    value: object,
    field: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Parse an exact raw integer without silently truncating fractional values."""
    if isinstance(value, (bool, bytes, bytearray)) or value is None:
        raise ValueError(f"{field} must be an exact integer")
    if isinstance(value, str):
        if not _INTEGER_TEXT.fullmatch(value):
            raise ValueError(f"{field} must be an exact integer")
        parsed = int(value)
    elif isinstance(value, numbers.Integral):
        parsed = int(value)
    elif isinstance(value, numbers.Real):
        numeric = float(value)
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError(f"{field} must be a finite exact integer")
        parsed = int(numeric)
    else:
        raise ValueError(f"{field} must be an exact integer")  # noqa: TRY004
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{field} is below the allowed range")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{field} is above the allowed range")
    return parsed


def strict_millisecond_timestamp(value: object, field: str) -> int:
    """Accept only plausible nonnegative Unix millisecond timestamps."""
    return strict_raw_integer(value, field, minimum=0, maximum=99_999_999_999_999)
