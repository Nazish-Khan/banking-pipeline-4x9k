"""
Extraction logic for the banking data pipeline.

Pulls raw data from the FDIC BankFind Suite API and the FRED API, lands
it in Cloud Storage, and loads it into BigQuery via the loader passed in
by main.py. Incremental: a small state file in GCS tracks what's already
been collected, so repeat runs only fetch what's new.

"""

import datetime
import json
import logging
import time

import requests
from google.api_core.exceptions import NotFound
from google.cloud import storage

log = logging.getLogger("extract")

STATE_PATH = "raw/_state/state.json"

FRED_SERIES = [
    "DPSACBW027SBOG",
    "TOTBKCR",
    "DRSFRMACBS",
    "FEDFUNDS",
]

FDIC_FIELDS = "CERT,NAME,STNAME,REPDTE,ASSET,DEP,NETINC,ROA,ROE,NPTLA"


class FDICFetchError(Exception):
    """Raised when the FDIC API cannot be reached or returns bad data."""


class FREDFetchError(Exception):
    """Raised when the FRED API cannot be reached or returns bad data."""


def fetch_json(url: str, params: dict, retries: int = 3, backoff: float = 2.0) -> dict:
    """GETs a URL and returns parsed JSON, retrying with exponential
    backoff on network errors. Raises the last error if all retries
    are exhausted."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            log.warning("attempt %s/%s failed for %s: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(backoff ** attempt)
    raise last_exc


def latest_quarter_end() -> str:
    """Returns the most recently closed calendar quarter end as
    YYYYMMDD. FDIC Call Report data for the current quarter usually
    isn't posted yet, so we ask for the last one that's already closed."""
    today = datetime.date.today()
    quarter_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
    candidates = [datetime.date(today.year, m, d) for m, d in quarter_ends]
    candidates += [datetime.date(today.year - 1, 12, 31)]
    past = [d for d in candidates if d < today]
    return max(past).strftime("%Y%m%d")


def fetch_fdic_financials(target_repdte: str) -> dict:
    url = "https://banks.data.fdic.gov/api/financials"
    params = {
        "filters": "REPDTE:" + target_repdte,
        "fields": FDIC_FIELDS,
        "limit": 10000,
        "format": "json",
    }
    log.info("fetching FDIC financials for REPDTE=%s", target_repdte)
    try:
        return fetch_json(url, params)
    except requests.RequestException as exc:
        raise FDICFetchError(f"failed to fetch FDIC financials for {target_repdte}") from exc


def fetch_fred_series(series_id: str, api_key: str, since: str | None) -> dict:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
    }
    if since:
        next_day = (datetime.date.fromisoformat(since) + datetime.timedelta(days=1)).isoformat()
        params["observation_start"] = next_day
        log.info("fetching FRED series %s since %s", series_id, next_day)
    else:
        log.info("fetching full history for FRED series %s (first run)", series_id)
    try:
        return fetch_json(url, params)
    except requests.RequestException as exc:
        raise FREDFetchError(f"failed to fetch FRED series {series_id}") from exc


def load_state(client: storage.Client, bucket_name: str) -> dict:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(STATE_PATH)
    try:
        return json.loads(blob.download_as_text())
    except NotFound:
        log.info("no state file found, treating this as the first run")
        return {"fred": {}, "fdic": {"last_repdte": None}}


def save_state(client: storage.Client, bucket_name: str, state: dict) -> None:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(STATE_PATH)
    blob.upload_from_string(json.dumps(state, indent=2), content_type="application/json")
    log.info("saved state to gs://%s/%s", bucket_name, STATE_PATH)


def upload_json(
    client: storage.Client, bucket_name: str, source: str, name: str, run_date: str, payload: dict
) -> None:
    blob_path = f"raw/{source}/dt={run_date}/{name}.json"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    log.info("wrote gs://%s/%s", bucket_name, blob_path)


def run(storage_client: storage.Client, bq_loader, bucket_name: str, fred_api_key: str) -> list[str]:
    """Runs one full extraction pass: FDIC plus every FRED series.

    Returns a list of source names that failed; an empty list means full
    success. A failure on one source doesn't stop the others, and
    whatever did succeed still gets saved to the state file so it isn't
    re-fetched next time.
    """
    run_date = datetime.date.today().isoformat()
    state = load_state(storage_client, bucket_name)
    failures: list[str] = []

    target_repdte = latest_quarter_end()
    if state["fdic"].get("last_repdte") == target_repdte:
        log.info("FDIC quarter %s already collected, skipping", target_repdte)
    else:
        try:
            fdic_data = fetch_fdic_financials(target_repdte)
            upload_json(storage_client, bucket_name, "fdic", "institutions_financials", run_date, fdic_data)
            bq_loader.load_payload("fdic_financials", "fdic", run_date, fdic_data)
            state["fdic"]["last_repdte"] = target_repdte
        except Exception as exc:
            log.error("FDIC extraction failed: %s", exc)
            failures.append("fdic")

    for series_id in FRED_SERIES:
        last_seen = state["fred"].get(series_id)
        try:
            data = fetch_fred_series(series_id, fred_api_key, last_seen)
            observations = data.get("observations", [])
            if not observations:
                log.info("no new observations for %s, nothing to save", series_id)
                continue
            upload_json(storage_client, bucket_name, "fred", series_id, run_date, data)
            bq_loader.load_payload("fred_series", series_id, run_date, data)
            state["fred"][series_id] = observations[-1]["date"]
        except Exception as exc:
            log.error("FRED extraction failed for %s: %s", series_id, exc)
            failures.append(f"fred:{series_id}")

    save_state(storage_client, bucket_name, state)
    return failures
