"""OpenLineage event construction and emission."""
from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.client import OpenLineageClientOptions
from openlineage.client.event_v2 import RunEvent, Run, Job, InputDataset, OutputDataset, RunState

from dlt_openlineage.datasets import (
    extract_info_to_input_datasets,
    load_info_to_output_datasets,
    pipeline_to_output_datasets_from_schema,
)
from dlt_openlineage.facets import (
    build_job_facets,
    build_run_facets,
    build_processing_engine_facet,
)

from dlt_openlineage.constants import PRODUCER

logger = logging.getLogger(__name__)


class OpenLineageEmitter:
    """Manages OpenLineage client and event emission."""

    def __init__(
        self,
        url: str,
        namespace: str = "dlt",
        api_key: t.Optional[str] = None,
    ) -> None:
        self.namespace = namespace
        self.url = url

        if url.startswith("console://"):
            from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

            transport = ConsoleTransport(ConsoleConfig())
            self.client = OpenLineageClient(transport=transport)
        elif api_key:
            self.client = OpenLineageClient(url=url, options=OpenLineageClientOptions(api_key=api_key))
        else:
            self.client = OpenLineageClient(url=url)

    def emit_start(
        self,
        pipeline_name: str,
        run_id: str,
        pipeline: t.Any = None,
    ) -> None:
        """Emit a START event for the pipeline run."""
        run_facets: t.Dict[str, t.Any] = {}
        try:
            engine_facet = build_processing_engine_facet()
            if engine_facet:
                run_facets["processing_engine"] = engine_facet
        except Exception:
            pass

        job_facets: t.Dict[str, t.Any] = {}
        try:
            job_facets = build_job_facets(pipeline=pipeline)
        except Exception:
            pass

        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=PRODUCER,
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=pipeline_name, facets=job_facets),
            inputs=[],
            outputs=[],
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event", event.eventType, exc_info=True)

    def emit_running(
        self,
        pipeline_name: str,
        run_id: str,
        step_name: str,
        extract_info: t.Any = None,
        pipeline: t.Any = None,
    ) -> None:
        """Emit a RUNNING event with extract results."""
        inputs: t.List[InputDataset] = []
        if extract_info is not None:
            try:
                inputs = extract_info_to_input_datasets(extract_info, self.namespace)
            except Exception:
                logger.debug("Failed to build input datasets from extract info", exc_info=True)

        run_facets = build_run_facets(step_name=step_name)

        event = RunEvent(
            eventType=RunState.RUNNING,
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=PRODUCER,
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=pipeline_name),
            inputs=inputs,
            outputs=[],
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event", event.eventType, exc_info=True)

    def emit_complete(
        self,
        pipeline_name: str,
        run_id: str,
        load_info: t.Any = None,
        pipeline: t.Any = None,
        row_counts: t.Optional[t.Dict[str, int]] = None,
        extract_info: t.Any = None,
    ) -> None:
        """Emit a COMPLETE event with load results."""
        outputs: t.List[OutputDataset] = []
        if load_info is not None:
            try:
                outputs = load_info_to_output_datasets(
                    load_info, self.namespace, pipeline=pipeline, row_counts=row_counts
                )
            except Exception:
                logger.debug(
                    "Failed to build output datasets from load info", exc_info=True
                )

        # If no outputs from load_info, try to get them from pipeline schema
        if not outputs and pipeline is not None:
            try:
                outputs = pipeline_to_output_datasets_from_schema(
                    pipeline, self.namespace, row_counts=row_counts
                )
            except Exception:
                logger.debug(
                    "Failed to build output datasets from pipeline schema", exc_info=True
                )

        inputs: t.List[InputDataset] = []
        if extract_info is not None:
            try:
                inputs = extract_info_to_input_datasets(extract_info, self.namespace)
            except Exception:
                logger.debug("Failed to build input datasets for COMPLETE event", exc_info=True)

        run_facets = build_run_facets(step_name="load", load_info=load_info)

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=PRODUCER,
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=pipeline_name),
            inputs=inputs,
            outputs=outputs,
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event", event.eventType, exc_info=True)

    def emit_fail(
        self,
        pipeline_name: str,
        run_id: str,
        error: t.Any,
        step_name: str = "",
        pipeline: t.Any = None,
    ) -> None:
        """Emit a FAIL event with error details."""
        from openlineage.client.facet_v2 import error_message_run

        error_message = str(error) if error else "Unknown error"
        stack_trace = None
        if isinstance(error, BaseException) and error.__traceback__ is not None:
            import traceback

            stack_trace = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )
        elif isinstance(error, str) and error:
            # dlt always stringifies exceptions in step_exception
            stack_trace = error

        run_facets: t.Dict[str, t.Any] = {
            "errorMessage": error_message_run.ErrorMessageRunFacet(
                message=error_message,
                programmingLanguage="python",
                stackTrace=stack_trace,
            )
        }

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=PRODUCER,
            run=Run(runId=run_id, facets=run_facets),
            job=Job(namespace=self.namespace, name=pipeline_name),
            inputs=[],
            outputs=[],
        )
        try:
            self.client.emit(event)
        except Exception:
            logger.warning("Failed to emit %s event", event.eventType, exc_info=True)
