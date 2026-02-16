"""Facet builders for dlt-openlineage."""
from __future__ import annotations

import logging
import typing as t

from dlt_openlineage.constants import PRODUCER

logger = logging.getLogger(__name__)


def build_processing_engine_facet() -> t.Optional[t.Any]:
    """Build a ProcessingEngineRunFacet with dlt version info."""
    try:
        from openlineage.client.facet_v2 import processing_engine_run
        import dlt

        dlt_version = getattr(dlt, "__version__", "unknown")
        from dlt_openlineage import __version__ as adapter_version

        return processing_engine_run.ProcessingEngineRunFacet(
            version=dlt_version,
            name="dlt",
            openlineageAdapterVersion=adapter_version,
        )
    except Exception:
        return None


def build_job_facets(pipeline: t.Any = None) -> t.Dict[str, t.Any]:
    """Build job facets for a dlt pipeline."""
    facets: t.Dict[str, t.Any] = {}

    try:
        from openlineage.client.facet_v2 import job_type_job

        facets["jobType"] = job_type_job.JobTypeJobFacet(
            processingType="BATCH",
            integration="DLT",
            jobType="PIPELINE",
        )
    except Exception:
        pass

    return facets


def build_run_facets(
    step_name: str = "",
    load_info: t.Any = None,
) -> t.Dict[str, t.Any]:
    """Build run facets for a pipeline step."""
    facets: t.Dict[str, t.Any] = {}

    execution_data: t.Dict[str, t.Any] = {}

    if step_name:
        execution_data["step"] = step_name

    if load_info is not None:
        try:
            execution_data["destination_type"] = str(load_info.destination_type)
            execution_data["destination_name"] = str(load_info.destination_name)
            execution_data["dataset_name"] = str(load_info.dataset_name)

            # Count loaded jobs
            total_jobs = 0
            failed_jobs = 0
            for pkg in load_info.load_packages:
                total_jobs += len(pkg.jobs.get("completed_jobs", []))
                failed_jobs += len(pkg.jobs.get("failed_jobs", []))
            execution_data["total_jobs"] = total_jobs
            execution_data["failed_jobs"] = failed_jobs
        except Exception:
            pass

    if execution_data:
        facets["dlt_execution"] = {
            "_producer": PRODUCER,
            "_schemaURL": f"{PRODUCER}#DltExecutionRunFacet",
            **execution_data,
        }

    return facets
