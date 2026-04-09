
import dlt
from .cso import cso_source

cso_source = cso_source()
cso_pipeline = dlt.pipeline(
    pipeline_name="rest_api_cso",
    destination='duckdb',
    dataset_name="raw_cso",
)
