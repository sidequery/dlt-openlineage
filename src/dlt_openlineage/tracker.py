"""OpenLineage tracker for dlt pipelines.

Implements dlt's SupportsTracking protocol to emit OpenLineage events
during pipeline execution.
"""
from __future__ import annotations

import uuid
import logging
import typing as t

if t.TYPE_CHECKING:
    from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace
    from dlt.pipeline.typing import TPipelineStep
    from dlt.common.pipeline import SupportsPipeline

from dlt_openlineage.emitter import OpenLineageEmitter

logger = logging.getLogger(__name__)


class OpenLineageTracker:
    """Tracks dlt pipeline execution and emits OpenLineage events.

    Implements the SupportsTracking protocol. Register by appending
    to dlt.pipeline.trace.TRACKING_MODULES.
    """

    def __init__(self, emitter: OpenLineageEmitter) -> None:
        self._emitter = emitter
        self._run_id: t.Optional[str] = None
        self._pipeline_name: t.Optional[str] = None
        self._normalize_row_counts: t.Dict[str, int] = {}
        self._extract_info: t.Any = None
        self._terminal_emitted: bool = False
        self._had_failure: bool = False

    def on_start_trace(
        self,
        trace: "PipelineTrace",
        step: "TPipelineStep",
        pipeline: "SupportsPipeline",
    ) -> None:
        """Called when a pipeline trace begins. Emits a START event."""
        self._run_id = str(uuid.uuid4())
        self._pipeline_name = pipeline.pipeline_name
        self._normalize_row_counts = {}
        self._extract_info = None
        self._terminal_emitted = False
        self._had_failure = False

        try:
            self._emitter.emit_start(
                pipeline_name=self._pipeline_name,
                run_id=self._run_id,
                pipeline=pipeline,
            )
        except Exception:
            logger.debug("Failed to emit START event", exc_info=True)

    def on_start_trace_step(
        self,
        trace: "PipelineTrace",
        step: "TPipelineStep",
        pipeline: "SupportsPipeline",
    ) -> None:
        """Called before each pipeline step (extract/normalize/load)."""
        # No event emitted on step start; we emit on step end with results.
        pass

    def on_end_trace_step(
        self,
        trace: "PipelineTrace",
        step: "PipelineStepTrace",
        pipeline: "SupportsPipeline",
        step_info: t.Any,
        send_state: bool,
    ) -> None:
        """Called after each pipeline step completes.

        Note: here ``step`` is a PipelineStepTrace object (not a string).
        The step name string is available as ``step.step``.
        """
        if self._run_id is None:
            return

        step_name = step.step

        try:
            # Check for step failure
            if step.step_exception is not None:
                self._emitter.emit_fail(
                    pipeline_name=self._pipeline_name,
                    run_id=self._run_id,
                    error=step.step_exception,
                    step_name=step_name,
                    pipeline=pipeline,
                )
                self._terminal_emitted = True
                self._had_failure = True
                return

            if step_name == "extract" and step_info is not None:
                self._extract_info = step_info
                self._emitter.emit_running(
                    pipeline_name=self._pipeline_name,
                    run_id=self._run_id,
                    step_name="extract",
                    extract_info=step_info,
                    pipeline=pipeline,
                )

            elif step_name == "normalize" and step_info is not None:
                # Store row counts for use in the load complete event
                try:
                    self._normalize_row_counts = step_info.row_counts or {}
                except Exception:
                    pass

            elif step_name == "load" and step_info is not None:
                self._emitter.emit_complete(
                    pipeline_name=self._pipeline_name,
                    run_id=self._run_id,
                    load_info=step_info,
                    pipeline=pipeline,
                    row_counts=self._normalize_row_counts,
                    extract_info=self._extract_info,
                )
                self._terminal_emitted = True

        except Exception:
            logger.debug("Failed to emit event for step %s", step_name, exc_info=True)

    def on_end_trace(
        self,
        trace: "PipelineTrace",
        pipeline: "SupportsPipeline",
        send_state: bool,
    ) -> None:
        """Called when the pipeline trace ends. Emits terminal event if needed and cleans up state."""
        if self._run_id is not None and not self._terminal_emitted:
            try:
                if self._had_failure:
                    self._emitter.emit_fail(
                        pipeline_name=self._pipeline_name,
                        run_id=self._run_id,
                        error="Pipeline failed",
                        step_name="unknown",
                        pipeline=pipeline,
                    )
                else:
                    self._emitter.emit_complete(
                        pipeline_name=self._pipeline_name,
                        run_id=self._run_id,
                        pipeline=pipeline,
                        row_counts=self._normalize_row_counts,
                        extract_info=self._extract_info,
                    )
            except Exception:
                logger.debug("Failed to emit terminal event in on_end_trace", exc_info=True)

        self._run_id = None
        self._pipeline_name = None
        self._normalize_row_counts = {}
        self._extract_info = None
        self._terminal_emitted = False
        self._had_failure = False
