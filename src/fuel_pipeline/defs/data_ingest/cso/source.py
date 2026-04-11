import datetime
from typing import Any

import dlt
from dagster import get_dagster_logger
from dlt.sources.helpers import requests

CSO_BASE_URL = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset"


@dlt.source(name="cso", max_table_nesting=0)
def cso_source(tables: list[str] | None = None) -> Any:
    """
    CSO PxStat JSON-stat source.

    Fetches raw JSON-stat 2.0 responses from the CSO API and yields them
    as-is for downstream parsing with pyjstat and dbt.
    """
    # 1. Initialize the Dagster logger
    logger = get_dagster_logger()

    if tables is None:
        tables = ["CPM02"]

    def _make_resource(table_id: str):

        @dlt.resource(
            name=table_id.lower(),
            write_disposition="append",
            columns={
                "extraction_timestamp": {"data_type": "timestamp"},
                "cso_updated_at": {"data_type": "timestamp"},
                "raw_payload": {"data_type": "json"},
            },
        )
        def _resource():

            # Log the start of an external API call
            logger.info(f"Initiating CSO API request for table: {table_id}")

            # 1.0 - API request
            response = requests.get(f"{CSO_BASE_URL}/{table_id}/JSON-stat/2.0/en")
            response.raise_for_status()

            data = response.json()
            assert data, f"Empty response from CSO API for {table_id}"

            # 2.0 - Resource State Management & Pipeline execution
            last_val = dlt.current.resource_state().get("last_updated")
            current_updated = data.get("updated")

            # Safety check
            if not current_updated:
                # Log errors right before they raise for easier debugging in the UI
                logger.error(
                    f"Payload validation failed: 'updated' field missing for {table_id}"
                )
                raise ValueError(
                    f"The 'updated' field is missing from the CSO {table_id} payload!"
                )

            # 2.3 - Run pipeline if changed
            if current_updated != last_val:
                logger.info(f"New data detected for {table_id}. Yielding payload.")

                # Wrap the payload to enforce a strict, 3-column raw table schema
                yield {
                    "extraction_timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ),
                    "cso_updated_at": current_updated,
                    "raw_payload": data,  # DLT can be configured to store this as JSONB
                }

                dlt.current.resource_state()["last_updated"] = current_updated
            else:
                # Log skipped executions due to idempotency
                logger.info(
                    f"No new updates for {table_id} (Last updated: {last_val}). Skipping append."
                )

        return _resource

    for table_id in tables:
        yield _make_resource(table_id)
