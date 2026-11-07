"""Writes API responses to the raw zone exactly as they came back.

Layout:
    <raw_root>/<format>/<ResourceType>/date=YYYY-MM-DD/run_id=<id>/page_0001.json

`date` is the day window that was requested (the _lastUpdated day), so every
resource has one folder per loaded day and a re-run only adds a new run_id folder.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Tuple


def raw_page_path(root: str, payload_format: str, resource: str, window_date: date, run_id: str, page_number: int) -> Path:
    return (
        Path(root)
        / payload_format
        / resource
        / f"date={window_date.isoformat()}"
        / f"run_id={run_id}"
        / f"page_{page_number:04d}.{payload_format}"
    )


def write_raw_page(
    root: str,
    payload_format: str,
    resource: str,
    window_date: date,
    run_id: str,
    page_number: int,
    body: str,
) -> Tuple[str, datetime, int]:
    """Save the body untouched. Returns (path, saved_at_utc, size_in_bytes)."""
    path = raw_page_path(root, payload_format, resource, window_date, run_id, page_number)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = body.encode("utf-8")
    path.write_bytes(data)
    return str(path), datetime.now(timezone.utc), len(data)
