"""Config loading and small naming helpers shared by every notebook."""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

_DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "pipeline_config.yml"


def _deep_update(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def load_config(path: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Read the YAML config. `overrides` is a nested dict merged on top."""
    cfg_path = Path(path or os.environ.get("FHIR_PIPELINE_CONFIG") or _DEFAULT_PATH)
    with open(cfg_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    if overrides:
        cfg = _deep_update(copy.deepcopy(cfg), overrides)
    return cfg


def table_name(cfg: Dict[str, Any], layer: str, table: str) -> str:
    """Fully qualified name: catalog.schema.table"""
    storage = cfg["storage"]
    return f"{storage['catalog']}.{storage['schemas'][layer]}.{table.lower()}"


def meta_table(cfg: Dict[str, Any], key: str) -> str:
    return table_name(cfg, "meta", cfg["tables"][key])


def raw_root(cfg: Dict[str, Any]) -> str:
    storage = cfg["storage"]
    if storage.get("raw_root_override"):
        return str(storage["raw_root_override"]).rstrip("/")
    return f"/Volumes/{storage['catalog']}/{storage['schemas']['raw']}/{storage['raw_volume']}"


def file_extension(payload_format: str) -> str:
    fmt = payload_format.lower()
    if fmt not in ("json", "xml"):
        raise ValueError(f"payload_format must be json or xml, got {payload_format!r}")
    return fmt
