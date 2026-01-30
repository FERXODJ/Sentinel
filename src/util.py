from __future__ import annotations

import json
from pathlib import Path

from .splynx_playwright import SplynxConfig


def load_config() -> SplynxConfig:
    root = Path(__file__).resolve().parents[1]
    cfg_path = root / "config.json"
    if not cfg_path.exists():
        cfg_path = root / "config.example.json"

    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    return SplynxConfig(
        login_url=str(raw["login_url"]),
        selectors=dict(raw.get("selectors", {})),
        tables=dict(raw.get("tables", {})),
        browser=dict(raw.get("browser", {})),
    )
