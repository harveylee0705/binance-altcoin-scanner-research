from __future__ import annotations

import pandas as pd
import pytest
from conftest import make_4h

from alt_hot_scanner.pipeline import build_vertical_slice
from alt_hot_scanner.universe.contracts import (
    filter_instrument_scope,
    records_from_exchange_info,
)
from alt_hot_scanner.utils.config import load_config


def _metadata(symbol: str, **overrides: object) -> dict:
    row = {
        "symbol": symbol,
        "base_asset": symbol.removesuffix("USDT"),
        "quote_asset": "USDT",
        "margin_asset": "USDT",
        "contract_type": "PERPETUAL",
        "underlying_type": "COIN",
        "underlying_subtype": ("synthetic-crypto",),
        "is_leveraged_token": False,
        "classification_provenance": "synthetic_test_fixture",
        "onboard_timestamp": pd.Timestamp("2020-01-01T00:00:00Z"),
        "delisting_announcement_timestamp": pd.NaT,
    }
    row.update(overrides)
    return row


def test_scope_uses_explicit_classification_not_name_suffix() -> None:
    config = load_config("config/research_v0_1.yaml")
    metadata = pd.DataFrame(
        [
            _metadata("JUPUSDT"),
            _metadata("SYRUPUSDT"),
            _metadata("USDCUSDT"),
            _metadata("BTCUPUSDT", is_leveraged_token=True),
        ]
    )
    scoped = filter_instrument_scope(metadata, config["universe"]["stablecoin_underlyings"])
    assert set(scoped["symbol"]) == {"JUPUSDT", "SYRUPUSDT"}


def test_pipeline_centrally_excludes_out_of_scope_contracts() -> None:
    config = load_config("config/research_v0_1.yaml")
    bars = pd.concat(
        [make_4h("BTCUSDT", 250, "2023-02-01"), make_4h("USDCUSDT", 250, "2023-02-01")],
        ignore_index=True,
    )
    contracts = pd.DataFrame([_metadata("BTCUSDT"), _metadata("USDCUSDT")])
    result = build_vertical_slice(bars, contracts, config, allowed_splits=["development"])
    assert set(result["symbol"]) == {"BTCUSDT"}
    assert result["hot_score"].isna().all()


def test_pipeline_fails_closed_on_unresolved_classification() -> None:
    config = load_config("config/research_v0_1.yaml")
    bars = make_4h("BTCUSDT", 50, "2023-02-01")
    contracts = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "onboard_timestamp": [pd.Timestamp("2020-01-01T00:00:00Z")],
            "delisting_announcement_timestamp": [pd.NaT],
        }
    )
    with pytest.raises(ValueError, match="classification is unresolved"):
        build_vertical_slice(bars, contracts, config, allowed_splits=["development"])


@pytest.mark.parametrize("subtype", [None, "DeFi", [], [""], [None]])
def test_exchange_info_missing_or_malformed_subtype_fails_closed(subtype: object) -> None:
    config = load_config("config/research_v0_1.yaml")
    item = {
        "symbol": "BTCUPUSDT",
        "baseAsset": "BTCUP",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "underlyingType": "COIN",
        "onboardDate": 1_600_000_000_000,
    }
    if subtype is not None:
        item["underlyingSubType"] = subtype
    normalized = records_from_exchange_info({"symbols": [item]})
    scoped = filter_instrument_scope(normalized, config["universe"]["stablecoin_underlyings"])
    assert scoped.empty
    assert pd.isna(normalized.iloc[0]["is_leveraged_token"])
    assert pd.isna(normalized.iloc[0]["classification_provenance"])


def test_exchange_info_missing_identity_field_is_quarantined_not_inferred() -> None:
    config = load_config("config/research_v0_1.yaml")
    item = {
        "symbol": "UNKNOWNUSDT",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "underlyingType": "COIN",
        "underlyingSubType": ["DeFi"],
    }
    normalized = records_from_exchange_info({"symbols": [item]})
    scoped = filter_instrument_scope(normalized, config["universe"]["stablecoin_underlyings"])
    assert scoped.empty
    assert pd.isna(normalized.iloc[0]["base_asset"])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbol", None),
        ("base_asset", ""),
        ("base_asset", None),
        ("classification_provenance", ""),
        ("classification_provenance", None),
    ],
)
def test_blank_required_classification_values_are_excluded(field: str, value: object) -> None:
    config = load_config("config/research_v0_1.yaml")
    row = _metadata("JUPUSDT")
    row[field] = value
    metadata = pd.DataFrame([row])
    scoped = filter_instrument_scope(metadata, config["universe"]["stablecoin_underlyings"])
    assert scoped.empty
