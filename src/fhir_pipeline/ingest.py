"""Ingestion of one FHIR resource: API -> raw files -> bronze Delta table, with logging."""

from __future__ import annotations

import traceback
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from .audit import get_watermark, log_api_calls, log_run, set_watermark
from .bronze import load_bronze
from .config import raw_root
from .fhir_client import FhirClient, utc_now
from .raw_writer import write_raw_page

Window = Tuple[datetime, datetime]


def _parse_day(value: Optional[str]) -> Optional[date]:
    if not value or not str(value).strip():
        return None
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()


def resolve_windows(
    cfg: Dict[str, Any],
    watermark: Optional[datetime],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    now: Optional[datetime] = None,
) -> List[Window]:
    """
    One window per calendar day (UTC). end_date is inclusive.

    - dates given     -> use them
    - no dates        -> continue from the watermark, or backfill N days when there is none
    """
    now = now or utc_now()
    today = now.date()
    first = _parse_day(start_date)
    last = _parse_day(end_date) or today

    if first is None:
        if watermark is not None:
            overlap = timedelta(hours=cfg["load"].get("watermark_overlap_hours", 0))
            first = (watermark - overlap).date()
        else:
            first = today - timedelta(days=cfg["load"]["default_backfill_days"] - 1)

    if first > last:
        return []

    windows: List[Window] = []
    day = first
    while day <= last:
        since = datetime.combine(day, time.min, tzinfo=timezone.utc)
        until = min(since + timedelta(days=1), now.replace(microsecond=0))
        if until > since:
            windows.append((since, until))
        day += timedelta(days=1)
    return windows


def fetch_to_raw(
    spark: SparkSession,
    cfg: Dict[str, Any],
    client: FhirClient,
    resource: str,
    run_id: str,
    windows: List[Window],
) -> Dict[str, Any]:
    """Call the API for each window, save every page untouched and log each call."""
    root = raw_root(cfg)
    max_pages = cfg["fhir"].get("max_pages_per_window")
    fmt = client.payload_format
    summary = {"pages": 0, "entries": 0, "windows": len(windows), "truncated_windows": 0}

    for since, until in windows:
        call_rows: List[Dict[str, Any]] = []
        last_page = None
        try:
            for page in client.fetch_pages(resource, since, until, max_pages=max_pages):
                path, saved_at, size = write_raw_page(root, fmt, resource, since.date(), run_id, page.page_number, page.body)
                call_rows.append(
                    {
                        "run_id": run_id,
                        "resource_type": resource,
                        "payload_format": fmt,
                        "window_start": since,
                        "window_end": until,
                        "page_number": page.page_number,
                        "request_url": page.url,
                        "http_status": page.status_code,
                        "attempts": page.attempts,
                        "entry_count": page.entry_count,
                        "bytes": size,
                        "file_path": path,
                        "called_at": page.called_at,
                        "saved_at": saved_at,
                        "error_message": None,
                    }
                )
                summary["pages"] += 1
                summary["entries"] += page.entry_count
                last_page = page
        except Exception as exc:
            call_rows.append(
                {
                    "run_id": run_id,
                    "resource_type": resource,
                    "payload_format": fmt,
                    "window_start": since,
                    "window_end": until,
                    "page_number": len(call_rows) + 1,
                    "request_url": getattr(exc, "url", None),
                    "http_status": None,
                    "called_at": utc_now(),
                    "error_message": str(exc)[:1500],
                }
            )
            raise
        finally:
            log_api_calls(spark, cfg, call_rows)

        if last_page is not None and max_pages and last_page.page_number >= max_pages and last_page.next_url:
            summary["truncated_windows"] += 1

    return summary


def run_resource_ingestion(
    spark: SparkSession,
    cfg: Dict[str, Any],
    resource: str,
    run_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    payload_format: Optional[str] = None,
    client: Optional[FhirClient] = None,
) -> Dict[str, Any]:
    fmt = payload_format or cfg["fhir"].get("payload_format", "json")
    client = client or FhirClient.from_config(cfg, payload_format=fmt)
    started = utc_now()

    try:
        watermark = get_watermark(spark, cfg, resource)
        windows = resolve_windows(cfg, watermark, start_date, end_date)
        if not windows:
            log_run(spark, cfg, run_id, resource, "raw+bronze", "SKIPPED", started, 0, 0, "nothing to load")
            return {"status": "SKIPPED"}

        fetched = fetch_to_raw(spark, cfg, client, resource, run_id, windows)
        loaded = load_bronze(spark, cfg, run_id, resource, fmt)

        note = f"{fetched['windows']} day window(s), {fetched['pages']} page(s)"
        if fetched["truncated_windows"]:
            note += f", {fetched['truncated_windows']} window(s) hit max_pages_per_window"
        log_run(spark, cfg, run_id, resource, "raw+bronze", "SUCCESS", started, loaded["rows_read"], loaded["rows_written"], note)

        set_watermark(spark, cfg, resource, windows[-1][1], run_id)
        return {"status": "SUCCESS", **fetched, **loaded}
    except Exception as exc:
        log_run(spark, cfg, run_id, resource, "raw+bronze", "FAILED", started, None, None, f"{exc}\n{traceback.format_exc()[-800:]}")
        raise
