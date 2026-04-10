# loads.py
import os
import dlt
from .cso.source import cso_source


def _pipeline_by_deployment(pipeline_name: str, dataset_name: str) -> dlt.Pipeline:
    deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")
    destinations = {
        "local": "duckdb",
        "testing": "postgres",
        "production": "postgres",
    }
    dataset_names = {
        "local": dataset_name,
        "testing": f"{dataset_name}_testing",
        "production": dataset_name,
    }
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destinations[deployment_name],
        dataset_name=dataset_names[deployment_name],
    )

# --- CSO ---
CSO_TABLES = ["CPM02"]  # Add tables here as needed, e.g. "CPM12"
cso_source = cso_source(tables=CSO_TABLES)
cso_pipeline = _pipeline_by_deployment("rest_api_cso", "raw_cso")

# --- Eurostat ---
# from .eurostat.source import eurostat_source
# eurostat_source = eurostat_source()
# eurostat_pipeline = _pipeline_by_deployment("rest_api_eurostat", "raw_eurostat")