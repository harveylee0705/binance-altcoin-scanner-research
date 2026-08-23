from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_config(path: str | Path) -> dict[str, Any]:
    """Load the frozen YAML configuration without applying implicit overrides."""
    with Path(path).open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if config.get("version") != "scanner-v0.1":
        raise ValueError("Only frozen scanner-v0.1 configuration is supported")
    return config
