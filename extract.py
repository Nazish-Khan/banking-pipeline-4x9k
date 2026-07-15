"""
Cloud Run job: extracts public banking-sector data from the FDIC BankFind
Suite API and the FRED API, and lands the raw JSON responses in Cloud
Storage, partitioned by ingestion date.
"""

import datetime
import json
import logging
import os
import sys
import time

import requests
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extract")

BUCKET_NAME = os.environ["GCS_BUCKET"]
FRED_API_KEY = os.environ["FRED_API_KEY"]

RUN_DATE = datetime.date.today().isoformat()

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


def fetch_fdic_financials() -> dict:
    url = "https://banks.data.fdic.gov/api/financials"
    params = {
        "filters": "REPDTE:" + latest_quarter_end(),
        "fields": FDIC_FIELDS,
        "limit": 10000,
        "format": "json",
    }
    log.info("fetching FDIC financials for REPDTE=%s", params["filters"])
    return fetch_json(url, params)


def fetch_fred_series(series_id: str) -> dict:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
    }
    log.info("fetching FRED series %s", series_id)
    return fetch_json(url, params)


def latest_quarter_end() -> str:
    today = datetime.date.today()
    quarter_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
    candidates = [datetime.date(today.year, m, d) for m, d in quarter_ends]
    candidates += [datetime.date(today.year - 1, 12, 31)]
    past = [d for d in candidates if d < today]
    return max(past).strftime("%Y%m%d")


def upload_json(client: storage.Client, source: str, name: str, payload: dict) -> None:
    blob_path = f"raw/{source}/dt={RUN_DATE}/{name}.json"
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    log.info("wrote gs://%s/%s", BUCKET_NAME, blob_path)


def main() -> int:
    client = storage.Client()
    failures = []

    try:
        fdic_data = fetch_fdic_financials()
        upload_json(client, "fdic", "institutions_financials", fdic_data)
    except Exception as exc:
        log.error("FDIC extraction failed: %s", exc)
        failures.append("fdic")

    for series_id in FRED_SERIES:
        try:
            data = fetch_fred_series(series_id)
            upload_json(client, "fred", series_id, data)
        except Exception as exc:
            log.error("FRED extraction failed for %s: %s", series_id, exc)
            failures.append(f"fred:{series_id}")

    if failures:
        log.error("extraction run completed with failures: %s", failures)
        return 1

    log.info("extraction run completed successfully for %s", RUN_DATE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
