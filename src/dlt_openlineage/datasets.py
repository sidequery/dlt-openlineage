"""Convert dlt metadata to OpenLineage datasets."""
from __future__ import annotations

import logging
import typing as t

from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.facet_v2 import schema_dataset, output_statistics_output_dataset

logger = logging.getLogger(__name__)


def _get_destination_namespace(load_info: t.Any) -> str:
    """Extract a namespace from load info destination details."""
    try:
        dest = load_info.destination_type
        dest_name = load_info.destination_name
        if hasattr(load_info, "destination_fingerprint") and load_info.destination_fingerprint:
            return f"{dest}://{load_info.destination_fingerprint}"
        return f"{dest}://{dest_name}"
    except Exception:
        return "unknown"


def _get_dataset_name(load_info: t.Any, table_name: str) -> str:
    """Build a dataset name from load info and table name."""
    try:
        dataset_name = load_info.dataset_name
        if dataset_name:
            return f"{dataset_name}.{table_name}"
    except Exception:
        pass
    return table_name


def _schema_to_facet(
    pipeline: t.Any, table_name: str
) -> t.Optional[schema_dataset.SchemaDatasetFacet]:
    """Build a SchemaDatasetFacet from dlt pipeline schema for a table."""
    try:
        s = pipeline.default_schema
        tables = s.tables
        if table_name not in tables:
            return None

        table = tables[table_name]
        columns = table.get("columns", {})
        if not columns:
            return None

        fields = []
        for col_name, col_info in columns.items():
            col_type = col_info.get("data_type", "unknown")
            fields.append(schema_dataset.SchemaDatasetFacetFields(name=col_name, type=col_type))

        return schema_dataset.SchemaDatasetFacet(fields=fields)
    except Exception:
        return None


def extract_info_to_input_datasets(
    extract_info: t.Any, namespace: str
) -> t.List[InputDataset]:
    """Convert dlt ExtractInfo to OpenLineage InputDatasets.

    ExtractInfo.metrics is Dict[str, List[ExtractMetrics]] where
    ExtractMetrics is a TypedDict with keys including ``table_metrics``
    (Dict[str, DataWriterMetrics] keyed by table name) and
    ``resource_metrics`` (Dict[str, DataWriterAndCustomMetrics] keyed by
    resource name).
    """
    table_names: t.Set[str] = set()

    try:
        for load_id_metrics in extract_info.metrics.values():
            for extract_metric in load_id_metrics:
                # Extract table names from table_metrics keys
                tbl_metrics = extract_metric.get("table_metrics", {})
                for tbl_name in tbl_metrics:
                    if tbl_name and not tbl_name.startswith("_dlt"):
                        table_names.add(tbl_name)

                # Also try resource_metrics keys as fallback
                if not tbl_metrics:
                    res_metrics = extract_metric.get("resource_metrics", {})
                    for res_name in res_metrics:
                        if res_name and not res_name.startswith("_dlt"):
                            table_names.add(res_name)
    except Exception:
        logger.debug("Failed to extract input datasets from extract info", exc_info=True)

    return [
        InputDataset(namespace=namespace, name=name, facets={})
        for name in sorted(table_names)
    ]


def load_info_to_output_datasets(
    load_info: t.Any,
    namespace: str,
    pipeline: t.Any = None,
    row_counts: t.Optional[t.Dict[str, int]] = None,
) -> t.List[OutputDataset]:
    """Convert dlt LoadInfo to OpenLineage OutputDatasets.

    Iterates over completed jobs in each load package to find loaded
    table names. Enriches with schema and row count facets when available.
    """
    outputs = []
    dest_namespace = _get_destination_namespace(load_info)

    try:
        # Get loaded table names from load packages
        loaded_tables: t.Set[str] = set()
        for load_package in load_info.load_packages:
            for job in load_package.jobs.get("completed_jobs", []):
                table_name = job.job_file_info.table_name
                if table_name and not table_name.startswith("_dlt"):
                    loaded_tables.add(table_name)

        for table_name in sorted(loaded_tables):
            dataset_name = _get_dataset_name(load_info, table_name)
            facets: t.Dict[str, t.Any] = {}

            # Add schema facet if pipeline available
            if pipeline is not None:
                schema_facet = _schema_to_facet(pipeline, table_name)
                if schema_facet:
                    facets["schema"] = schema_facet

            # Add output statistics
            output_facets: t.Dict[str, t.Any] = {}
            if row_counts and table_name in row_counts:
                output_facets["outputStatistics"] = (
                    output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                        rowCount=row_counts[table_name],
                    )
                )

            outputs.append(
                OutputDataset(
                    namespace=dest_namespace,
                    name=dataset_name,
                    facets=facets,
                    outputFacets=output_facets,
                )
            )

    except Exception:
        logger.debug("Failed to build output datasets from load info", exc_info=True)

    return outputs


def pipeline_to_output_datasets_from_schema(
    pipeline: t.Any,
    namespace: str,
    row_counts: t.Optional[t.Dict[str, int]] = None,
) -> t.List[OutputDataset]:
    """Fallback: build output datasets from pipeline schema when load_info isn't sufficient."""
    outputs = []
    try:
        s = pipeline.default_schema
        for table_name, _table in s.tables.items():
            if table_name.startswith("_dlt"):
                continue

            facets: t.Dict[str, t.Any] = {}
            schema_facet = _schema_to_facet(pipeline, table_name)
            if schema_facet:
                facets["schema"] = schema_facet

            output_facets: t.Dict[str, t.Any] = {}
            if row_counts and table_name in row_counts:
                output_facets["outputStatistics"] = (
                    output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                        rowCount=row_counts[table_name],
                    )
                )

            outputs.append(
                OutputDataset(
                    namespace=namespace,
                    name=table_name,
                    facets=facets,
                    outputFacets=output_facets,
                )
            )
    except Exception:
        logger.debug("Failed to build output datasets from pipeline schema", exc_info=True)
    return outputs
