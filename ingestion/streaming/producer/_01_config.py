from typing import Any

import yaml


def load_config(env: str, config_path: str) -> dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        full_cfg = yaml.safe_load(f)
    if env not in full_cfg:
        raise KeyError(f"Environment '{env}' not found in {config_path}")
    return full_cfg[env]
