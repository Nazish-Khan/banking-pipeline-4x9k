"""
Cloud Run job: extracts public banking-sector data from the FDIC BankFind
Suite API and the FRED API, and lands raw JSON in Cloud Storage.

Incremental behavior:
- FRED: only fetches observations newer than the last saved date per series,
  using FRED's own observation_start filter.
- FDIC: only fetches a quarter's financials if that exact quarter hasn't
  been saved before (Call Report data is inherently quarterly).
- A small state file at raw/_state/state.json in the bucket tracks what's
  already been collected, and is read at the start and rewritten at the
  end of every run.
"""

import datetime
import json
import logging
import os
import sys
import time

import requests
from google.cloud import storage
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extract")

BUCKET_NAME = os.environ["GCS_BUCKET"]
FRED_API_KEY = os.environ["FRED_API_KEY"]

RUN_DATE = datetime.date.today().isoformat()
STATE_PATH = "raw/_state/state.json"

FRED_SERIES = [
    "DPSACBW027SBOG",
    "TOTBKCR",
    "DRSFRMACBS",
    "FEDFUNDS",
]

FDIC_FIELDS = "CERT,NAME,STNAME,REPDTE,ASSET,DEP,NETINC,ROA,ROE,NPTLA"


def fetch_json(url: str, params: dict, retries: int = 3, backoff: float = 2.0) -> dict:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("attempt %s/%s failed for %s: %s", attempt, retries, url, exc)
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)


def load_state(client: storage.Client) -> dict:
    """Reads the small JSON file that tracks what's already been collected.
    Returns an empty state on the very first run, when no file exists yet."""
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(STATE_PATH)
    try:
        return json.loads(blob.download_as_text())
    except NotFound:
        log.info("no state file found, treating this as the first run")
        return {"fred": {}, "fdic": {"last_repdte": None}}


def save_state(client: storage.Client, state: dict) -> None:
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(STATE_PATH)
    blob.upload_from_string(json.dumps(state, indent=2), content_type="application/json")
    log.info("saved state to gs://%s/%s", BUCKET_NAME, STATE_PATH)


def latest_quarter_end() -> str:
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
    return fetch_json(url, params)


def fetch_fred_series(series_id: str, since: str | None) -> dict:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
    }
    if since:
        # ask FRED for only observations strictly after the last one we saved
        next_day = (datetime.date.fromisoformat(since) + datetime.timedelta(days=1)).isoformat()
        params["observation_start"] = next_day
        log.info("fetching FRED series %s since %s", series_id, next_day)
    else:
        log.info("fetching full history for FRED series %s (first run)", series_id)
    return fetch_json(url, params)


def upload_json(client: storage.Client, source: str, name: str, payload: dict) -> None:
    blob_path = f"raw/{source}/dt={RUN_DATE}/{name}.json"
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    log.info("wrote gs://%s/%s", BUCKET_NAME, blob_path)


def main() -> int:
    client = storage.Client()
    state = load_state(client)
    failures = []

    # --- FDIC: skip entirely if we already have this exact quarter ---
    target_repdte = latest_quarter_end()
    if state["fdic"].get("last_repdte") == target_repdte:
        log.info("FDIC quarter %s already collected, skipping", target_repdte)
    else:
        try:
            fdic_data = fetch_fdic_financials(target_repdte)
            upload_json(client, "fdic", "institutions_financials", fdic_data)
            state["fdic"]["last_repdte"] = target_repdte
        except Exception as exc:
            log.error("FDIC extraction failed: %s", exc)
            failures.append("fdic")

    # --- FRED: fetch only observations newer than last saved per series ---
    for series_id in FRED_SERIES:
        last_seen = state["fred"].get(series_id)
        try:
            data = fetch_fred_series(series_id, last_seen)
            observations = data.get("observations", [])
            if not observations:
                log.info("no new observations for %s, nothing to save", series_id)
                continue
            upload_json(client, "fred", series_id, data)
            state["fred"][series_id] = observations[-1]["date"]
        except Exception as exc:
            log.error("FRED extraction failed for %s: %s", series_id, exc)
            failures.append(f"fred:{series_id}")

    save_state(client, state)

    if failures:
        log.error("extraction run completed with failures: %s", failures)
        return 1

    log.info("extraction run completed successfully for %s", RUN_DATE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
