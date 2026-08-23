from __future__ import annotations

import numpy as np
import pytest
from conftest import make_1h

from alt_hot_scanner.data.normalize import validate_normalized_1h


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("high", 98.0),
        ("low", 102.0),
        ("open", 0.0),
        ("close", -1.0),
        ("base_volume", -1.0),
        ("quote_volume", -1.0),
        ("taker_buy_base_volume", -1.0),
        ("taker_buy_quote_volume", -1.0),
        ("trade_count", -1),
    ],
)
def test_structurally_invalid_normalized_values_are_rejected(column: str, value: float) -> None:
    frame = make_1h("ETHUSDT", 2)
    frame.loc[0, column] = value
    with pytest.raises(ValueError):
        validate_normalized_1h(frame)


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf])
@pytest.mark.parametrize(
    "column",
    [
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "trade_count",
    ],
)
def test_non_finite_retained_numeric_fields_are_rejected(column: str, value: float) -> None:
    frame = make_1h("ETHUSDT", 1)
    if column == "trade_count":
        frame[column] = frame[column].astype("float64")
    frame.loc[0, column] = value
    with pytest.raises(ValueError, match="Non-finite"):
        validate_normalized_1h(frame)


def test_extreme_but_internally_valid_observation_is_accepted() -> None:
    frame = make_1h("ETHUSDT", 1)
    frame.loc[0, ["open", "close"]] = [1e-12, 1e12]
    frame.loc[0, ["low", "high"]] = [1e-15, 1e15]
    frame.loc[0, ["base_volume", "quote_volume"]] = [1e200, 1e250]
    frame.loc[0, ["taker_buy_base_volume", "taker_buy_quote_volume"]] = [1e199, 1e249]
    validate_normalized_1h(frame)
