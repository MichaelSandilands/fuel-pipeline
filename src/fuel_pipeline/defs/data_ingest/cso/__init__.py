from typing import Any

import dlt
import requests


@dlt.source(name="cso", max_table_nesting=0)
def cso_source() -> Any:
    
    @dlt.resource(name="cpm02", write_disposition="append")
    def cpm02():
        response = requests.get(
            "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/CPM02/JSON-stat/2.0/en"
        )
        response.raise_for_status()
        assert response.json(), "Empty response from CSO API"
        yield {"raw_response": response.json()}

    yield cpm02
