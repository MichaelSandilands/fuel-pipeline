from typing import Any
import dlt
from dlt.sources.helpers import requests
from dagster import get_dagster_logger

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

        @dlt.resource(name=table_id.lower(), write_disposition="append")
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
                logger.error(f"Payload validation failed: 'updated' field missing for {table_id}")
                raise ValueError(f"The 'updated' field is missing from the CSO {table_id} payload!")

            # 2.3 - Run pipeline if changed
            if current_updated != last_val:
                # Log successful state changes / data updates
                logger.info(f"New data detected for {table_id}. Previous: {last_val}, Current: {current_updated}. Yielding payload.")
                yield data

                dlt.current.resource_state()["last_updated"] = current_updated
            else:
                # Log skipped executions due to idempotency
                logger.info(f"No new updates for {table_id} (Last updated: {last_val}). Skipping append.")

        return _resource

    for table_id in tables:
        yield _make_resource(table_id)