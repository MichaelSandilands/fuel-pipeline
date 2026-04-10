from typing import Any

import dlt
from dlt.sources.helpers import requests


@dlt.source(name="cso", max_table_nesting=0)
def cso_source() -> Any:
    
    @dlt.resource(name="cpm02", write_disposition="append")
    def cpm02():
        # 1.0 - API request
        response = requests.get(
            "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/CPM02/JSON-stat/2.0/en"
        )
        response.raise_for_status()
        
        # Parse json store in variable
        data = response.json()
        assert data, "Empty response from CSO API"

        # 2.0 - Resource State Management & Pipeline execution
        # We want Idempotency (Only append if there is an actual change to the data)
        # The cso json has an "updated" field. If this field is changed, we can run the pipeline

        # 2.1 - Get last run value
        last_val = dlt.current.resource_state().get("last_updated")

        # 2.2 Get current "updated" value
        current_updated = data.get("updated")
        # Safety check: ensure the field actually exists
        if not current_updated:
            raise ValueError("The 'updated' field is missing from the CSO payload!")
        
        # 2.3 - Run pipeline if changed
        if current_updated != last_val:
            print(f"New data detected! Last updated by CSO at: {current_updated}. Appending.")
            
            # Yield the 'data' variable directly
            yield data
            
            # Change the state to the 'current_updated' value
            dlt.current.resource_state()["last_updated"] = current_updated
        # Skip if not changed
        else:
            print(f"Skipping append. Data has not been updated by CSO since {last_val}.")

    yield cpm02