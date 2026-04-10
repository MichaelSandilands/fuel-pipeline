from typing import Any

import dlt
from dlt.sources.helpers import requests

CSO_BASE_URL = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset"


@dlt.source(name="cso", max_table_nesting=0)
def cso_source(tables: list[str] | None = None) -> Any:
    """
    CSO PxStat JSON-stat source.

    Fetches raw JSON-stat 2.0 responses from the CSO API and yields them
    as-is for downstream parsing with pyjstat and dbt.

    Args:
        tables: List of CSO table IDs to ingest (e.g. ["CPM02", "CPM12"]).
                Defaults to ["CPM02"] if not specified.
    """
    if tables is None:
        tables = ["CPM02"]

    def _make_resource(table_id: str):

        @dlt.resource(name=table_id.lower(), write_disposition="append")
        def _resource():

            # 1.0 - API request
            response = requests.get(f"{CSO_BASE_URL}/{table_id}/JSON-stat/2.0/en")
            response.raise_for_status()

            # Parse json, store in variable
            data = response.json()
            assert data, f"Empty response from CSO API for {table_id}"

            # 2.0 - Resource State Management & Pipeline execution
            # We want Idempotency (Only append if there is an actual change to the data)
            # The CSO json has an "updated" field. If this field has changed, we run the pipeline

            # 2.1 - Get last run value
            last_val = dlt.current.resource_state().get("last_updated")

            # 2.2 - Get current "updated" value
            current_updated = data.get("updated")

            # Safety check: ensure the field actually exists
            if not current_updated:
                raise ValueError(f"The 'updated' field is missing from the CSO {table_id} payload!")

            # 2.3 - Run pipeline if changed
            if current_updated != last_val:
                print(f"[{table_id}] New data detected! Last updated by CSO at: {current_updated}. Appending.")

                # Yield the raw JSON-stat payload directly
                yield data

                # Update state to the current "updated" value
                dlt.current.resource_state()["last_updated"] = current_updated

            # Skip if not changed
            else:
                print(f"[{table_id}] Skipping append. Data has not been updated by CSO since {last_val}.")

        return _resource

    # Yield a resource for each table ID
    for table_id in tables:
        yield _make_resource(table_id)