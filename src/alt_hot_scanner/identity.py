from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence


class IdentityValidationError(ValueError):
    """An identity-bearing value is malformed or not in its canonical representation."""


_BINANCE_TOKEN = re.compile(r"[A-Z0-9]{1,64}")


def require_canonical_text(value: object, field: str, *, token: bool = False) -> str:
    """Validate text without silently trimming, recasing, or coercing caller input."""
    if type(value) is not str:
        raise IdentityValidationError(f"{field} must be a string")
    if not value or value != value.strip():
        raise IdentityValidationError(f"{field} must be nonempty and unpadded")
    if unicodedata.normalize("NFC", value) != value:
        raise IdentityValidationError(f"{field} must use canonical Unicode encoding")
    if not value.isascii():
        raise IdentityValidationError(f"{field} must contain ASCII characters only")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise IdentityValidationError(f"{field} must not contain control characters")
    if any(ord(character) < 0x20 or ord(character) > 0x7E for character in value):
        raise IdentityValidationError(f"{field} contains a noncanonical character")
    if token and _BINANCE_TOKEN.fullmatch(value) is None:
        raise IdentityValidationError(
            f"{field} must be an uppercase Binance identity token containing only A-Z and 0-9"
        )
    return value


def require_binance_token(value: object, field: str) -> str:
    return require_canonical_text(value, field, token=True)


def require_identity_sequence(value: object, field: str) -> tuple[str, ...]:
    """Validate a nonempty list/tuple of canonical descriptive identity labels."""
    if not isinstance(value, (list, tuple)) or not value:
        raise IdentityValidationError(f"{field} must be a nonempty list or tuple")
    return tuple(
        require_canonical_text(item, f"{field}[{index}]") for index, item in enumerate(value)
    )


def require_stablecoin_underlyings(value: object) -> frozenset[str]:
    """Validate configured stablecoin identities before using them for exclusion."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise IdentityValidationError("stablecoin_underlyings must be a nonempty sequence")
    tokens = [
        require_binance_token(item, f"stablecoin_underlyings[{index}]")
        for index, item in enumerate(value)
    ]
    if len(tokens) != len(set(tokens)):
        raise IdentityValidationError("stablecoin_underlyings must not contain duplicates")
    return frozenset(tokens)
