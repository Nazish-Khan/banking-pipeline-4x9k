"""
Loads raw JSON payloads into BigQuery. Each row stores the untouched API
response as a JSON string, plus a little tracking metadata, no parsing or
business logic here, that belongs in dbt downstream. This module's only
job is "get the payload into a queryable table reliably."
"""

import datetime
import json
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

log = logging.getLogger("loader")


class BigQueryLoadError(Exception):
    """Raised when a row fails to insert into BigQuery."""

RAW_TABLE_SCHEMA = [
    bigquery.SchemaField("run_date", "DATE"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    bigquery.SchemaField("source_name", "STRING"),
    bigquery.SchemaField("payload", "JSON"),
]


class BigQueryLoader:
    """Wraps the BigQuery client so extract.py can just call
    load_payload(...) without knowing anything about dataset or table
    setup. Creates the dataset and tables the first time they're needed,
    so there's no separate manual setup step required in BigQuery itself.
    """

    def __init__(self, dataset_id: str, location: str = "europe-west2"):
        self.client = bigquery.Client()
        self.dataset_id = dataset_id
        self.location = location
        self._ensure_dataset()

    def _ensure_dataset(self) -> None:
        dataset_ref = bigquery.DatasetReference(self.client.project, self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            log.info("created BigQuery dataset %s", self.dataset_id)

    def _ensure_table(self, table_name: str) -> bigquery.TableReference:
        table_ref = bigquery.DatasetReference(self.client.project, self.dataset_id).table(table_name)
        try:
            self.client.get_table(table_ref)
        except NotFound:
            table = bigquery.Table(table_ref, schema=RAW_TABLE_SCHEMA)
            self.client.create_table(table)
            log.info("created BigQuery table %s.%s", self.dataset_id, table_name)
        return table_ref

    def load_payload(self, table_name: str, source_name: str, run_date: str, payload: dict) -> None:
        """Inserts one row containing the full raw payload. table_name is
        typically the source, e.g. 'fdic_financials' or 'fred_series'."""
        table_ref = self._ensure_table(table_name)
        row = {
            "run_date": run_date,
            "loaded_at": datetime.datetime.utcnow().isoformat(),
            "source_name": source_name,
            "payload": json.dumps(payload),
        }
        errors = self.client.insert_rows_json(table_ref, [row])
        if errors:
            raise BigQueryLoadError(f"BigQuery insert errors for {table_name}: {errors}")
        log.info("loaded 1 row into %s.%s (source=%s)", self.dataset_id, table_name, source_name)
