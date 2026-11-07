"""Small FHIR REST client with paging, retries and incremental windows.

Only depends on `requests`, so it runs the same on a Spark driver or on a laptop.
"""

from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlencode

import requests

RETRY_STATUS = {429, 500, 502, 503, 504}
ACCEPT = {"json": "application/fhir+json", "xml": "application/fhir+xml"}


class FhirApiError(RuntimeError):
    pass


@dataclass
class Page:
    page_number: int
    url: str
    status_code: int
    called_at: datetime
    body: str
    entry_count: int
    next_url: Optional[str]
    total: Optional[int] = None
    attempts: int = 1
    extra: Dict[str, Any] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def fmt_instant(dt: datetime) -> str:
    """FHIR instant in UTC, e.g. 2026-11-05T00:00:00Z"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _local(tag: str) -> str:
    return tag.split("}")[-1]


def parse_bundle_meta(body: str, payload_format: str) -> Tuple[int, Optional[str], Optional[int]]:
    """Return (entry_count, next_url, total) from a searchset Bundle."""
    if payload_format == "json":
        data = json.loads(body)
        entries = data.get("entry") or []
        next_url = None
        for link in data.get("link") or []:
            if link.get("relation") == "next":
                next_url = link.get("url")
                break
        return len(entries), next_url, data.get("total")

    root = ET.fromstring(body)
    entries = [c for c in root if _local(c.tag) == "entry"]
    next_url = None
    total = None
    for child in root:
        name = _local(child.tag)
        if name == "total":
            try:
                total = int(child.get("value"))
            except (TypeError, ValueError):
                total = None
        if name == "link":
            rel = url = None
            for sub in child:
                if _local(sub.tag) == "relation":
                    rel = sub.get("value")
                if _local(sub.tag) == "url":
                    url = sub.get("value")
            if rel == "next":
                next_url = url
    return len(entries), next_url, total


class FhirClient:
    def __init__(
        self,
        base_url: str,
        payload_format: str = "json",
        page_size: int = 100,
        timeout: int = 60,
        max_retries: int = 5,
        backoff_seconds: float = 2.0,
        request_delay: float = 0.3,
        incremental_param: str = "_lastUpdated",
        sort_param: Optional[str] = "_lastUpdated",
        session: Optional[requests.Session] = None,
        sleep=time.sleep,
    ):
        if payload_format not in ACCEPT:
            raise ValueError("payload_format must be json or xml")
        self.base_url = base_url.rstrip("/")
        self.payload_format = payload_format
        self.page_size = page_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.request_delay = request_delay
        self.incremental_param = incremental_param
        self.sort_param = sort_param
        self.session = session or requests.Session()
        self._sleep = sleep

    @classmethod
    def from_config(cls, cfg: Dict[str, Any], payload_format: Optional[str] = None, **kwargs) -> "FhirClient":
        f = cfg["fhir"]
        return cls(
            base_url=f["base_url"],
            payload_format=payload_format or f.get("payload_format", "json"),
            page_size=f.get("page_size", 100),
            timeout=f.get("timeout_seconds", 60),
            max_retries=f.get("max_retries", 5),
            backoff_seconds=f.get("backoff_seconds", 2),
            request_delay=f.get("request_delay_seconds", 0.3),
            incremental_param=f.get("incremental_param", "_lastUpdated"),
            sort_param=f.get("sort_param"),
            **kwargs,
        )

    # ------------------------------------------------------------------ urls
    def build_search_url(self, resource: str, since: Optional[datetime], until: Optional[datetime]) -> str:
        params: List[Tuple[str, str]] = [("_count", str(self.page_size)), ("_format", self.payload_format)]
        if self.sort_param:
            params.append(("_sort", self.sort_param))
        if since is not None:
            params.append((self.incremental_param, f"ge{fmt_instant(since)}"))
        if until is not None:
            params.append((self.incremental_param, f"lt{fmt_instant(until)}"))
        return f"{self.base_url}/{resource}?{urlencode(params)}"

    # --------------------------------------------------------------- request
    def _get(self, url: str) -> Tuple[requests.Response, int]:
        headers = {"Accept": ACCEPT[self.payload_format], "User-Agent": "fhir-medallion-pipeline/1.0"}
        last_error: Optional[str] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, headers=headers, timeout=self.timeout)
            except requests.RequestException as exc:  # network level problem
                last_error = str(exc)
            else:
                if resp.status_code == 200:
                    return resp, attempt
                if resp.status_code not in RETRY_STATUS:
                    raise FhirApiError(f"HTTP {resp.status_code} for {url}: {resp.text[:300]}")
                last_error = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    self._sleep(min(int(retry_after), 60))
                    continue
            self._sleep(self.backoff_seconds * (2 ** (attempt - 1)))
        raise FhirApiError(f"Gave up after {self.max_retries} attempts for {url}: {last_error}")

    # ---------------------------------------------------------------- paging
    def fetch_pages(
        self,
        resource: str,
        since: Optional[datetime],
        until: Optional[datetime],
        max_pages: Optional[int] = None,
    ) -> Iterator[Page]:
        """Walk the Bundle `next` links until there are none left (or max_pages is hit)."""
        url: Optional[str] = self.build_search_url(resource, since, until)
        page_number = 0
        while url:
            page_number += 1
            called_at = utc_now()
            resp, attempts = self._get(url)
            body = resp.text
            entry_count, next_url, total = parse_bundle_meta(body, self.payload_format)
            yield Page(
                page_number=page_number,
                url=url,
                status_code=resp.status_code,
                called_at=called_at,
                body=body,
                entry_count=entry_count,
                next_url=next_url,
                total=total,
                attempts=attempts,
            )
            if max_pages and page_number >= max_pages:
                return
            url = next_url
            if url and self.request_delay:
                self._sleep(self.request_delay)
