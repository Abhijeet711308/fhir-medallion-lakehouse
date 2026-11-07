import json
from datetime import datetime, timezone

import pytest

from fhir_pipeline.config import load_config
from fhir_pipeline.fhir_client import FhirApiError, FhirClient, parse_bundle_meta
from fhir_pipeline.ingest import resolve_windows


class FakeResponse:
    def __init__(self, status=200, payload=None, text=None, headers=None):
        self.status_code = status
        self.text = text if text is not None else json.dumps(payload or {})
        self.headers = headers or {}


class FakeSession:
    """Returns queued responses in order and remembers the urls it was asked for."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
        return self.responses.pop(0)


def bundle(n_entries, next_url=None):
    data = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"fullUrl": f"x/{i}", "resource": {"resourceType": "Patient", "id": str(i)}} for i in range(n_entries)],
        "link": [{"relation": "self", "url": "self-url"}],
    }
    if next_url:
        data["link"].append({"relation": "next", "url": next_url})
    return data


def make_client(responses):
    session = FakeSession(responses)
    client = FhirClient(
        base_url="https://example.test/fhir",
        page_size=2,
        session=session,
        sleep=lambda s: None,
        max_retries=3,
    )
    return client, session


WINDOW = (datetime(2026, 11, 5, tzinfo=timezone.utc), datetime(2026, 11, 6, tzinfo=timezone.utc))


def test_search_url_has_window_and_paging_params():
    client, _ = make_client([])
    url = client.build_search_url("Patient", *WINDOW)
    assert url.startswith("https://example.test/fhir/Patient?")
    assert "_count=2" in url
    assert "_lastUpdated=ge2026-11-05T00%3A00%3A00Z" in url
    assert "_lastUpdated=lt2026-11-06T00%3A00%3A00Z" in url


def test_follows_next_links_until_the_end():
    client, session = make_client(
        [
            FakeResponse(payload=bundle(2, "https://example.test/page2")),
            FakeResponse(payload=bundle(2, "https://example.test/page3")),
            FakeResponse(payload=bundle(1)),
        ]
    )
    pages = list(client.fetch_pages("Patient", *WINDOW))
    assert [p.page_number for p in pages] == [1, 2, 3]
    assert [p.entry_count for p in pages] == [2, 2, 1]
    assert session.urls[1:] == ["https://example.test/page2", "https://example.test/page3"]


def test_max_pages_stops_early():
    client, session = make_client([FakeResponse(payload=bundle(2, "https://example.test/next"))] * 5)
    pages = list(client.fetch_pages("Patient", *WINDOW, max_pages=2))
    assert len(pages) == 2
    assert pages[-1].next_url == "https://example.test/next"


def test_retries_on_server_error_then_succeeds():
    client, session = make_client([FakeResponse(status=503), FakeResponse(status=429), FakeResponse(payload=bundle(1))])
    pages = list(client.fetch_pages("Patient", *WINDOW))
    assert len(pages) == 1
    assert pages[0].attempts == 3


def test_gives_up_after_max_retries():
    client, _ = make_client([FakeResponse(status=500)] * 3)
    with pytest.raises(FhirApiError):
        list(client.fetch_pages("Patient", *WINDOW))


def test_client_error_is_not_retried():
    client, session = make_client([FakeResponse(status=400, text="bad request")])
    with pytest.raises(FhirApiError):
        list(client.fetch_pages("Patient", *WINDOW))
    assert len(session.urls) == 1


def test_xml_bundle_meta():
    xml = (
        '<Bundle xmlns="http://hl7.org/fhir"><total value="7"/>'
        '<link><relation value="self"/><url value="s"/></link>'
        '<link><relation value="next"/><url value="https://example.test/n"/></link>'
        '<entry><resource><Patient><id value="1"/></Patient></resource></entry>'
        "</Bundle>"
    )
    count, nxt, total = parse_bundle_meta(xml, "xml")
    assert (count, nxt, total) == (1, "https://example.test/n", 7)


# ---------------------------------------------------------------- window logic
CFG = load_config()
NOW = datetime(2026, 11, 7, 10, 30, tzinfo=timezone.utc)


def test_first_run_backfills_three_days_ending_today():
    windows = resolve_windows(CFG, None, now=NOW)
    assert [w[0].date().isoformat() for w in windows] == ["2026-11-05", "2026-11-06", "2026-11-07"]
    # the last window is cut at "now" so we never claim to have read the future
    assert windows[-1][1] == NOW.replace(microsecond=0)


def test_explicit_dates_are_inclusive():
    windows = resolve_windows(CFG, None, "2026-11-01", "2026-11-02", now=NOW)
    assert len(windows) == 2
    assert windows[0][1] == datetime(2026, 11, 2, tzinfo=timezone.utc)


def test_continues_from_watermark():
    wm = datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)
    windows = resolve_windows(CFG, wm, now=NOW)
    assert [w[0].date().isoformat() for w in windows] == ["2026-11-06", "2026-11-07"]


def test_start_after_end_gives_nothing():
    assert resolve_windows(CFG, None, "2026-11-09", "2026-11-08", now=NOW) == []
