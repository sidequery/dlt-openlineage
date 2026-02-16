"""Tests for the OpenLineage tracker."""
from unittest.mock import MagicMock

from openlineage.client.event_v2 import RunState

from dlt_openlineage.tracker import OpenLineageTracker
from dlt_openlineage.emitter import OpenLineageEmitter


class TestOpenLineageTracker:
    def test_on_start_trace_emits_start(self, mock_pipeline, mock_openlineage_client):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        step = "run"  # TPipelineStep is a string literal

        tracker.on_start_trace(trace, step, mock_pipeline)

        assert tracker._run_id is not None
        assert tracker._pipeline_name == "test_pipeline"
        mock_openlineage_client.emit.assert_called_once()

        event = mock_openlineage_client.emit.call_args[0][0]
        assert event.eventType == RunState.START
        assert event.job.name == "test_pipeline"

    def test_on_end_trace_step_extract_emits_running(
        self, mock_pipeline, mock_extract_info, mock_openlineage_client
    ):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        # Start first
        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)
        mock_openlineage_client.emit.reset_mock()

        # Extract step: on_end_trace_step receives a PipelineStepTrace object
        step = MagicMock()
        step.step = "extract"
        step.step_exception = None

        tracker.on_end_trace_step(trace, step, mock_pipeline, mock_extract_info, False)

        mock_openlineage_client.emit.assert_called_once()
        event = mock_openlineage_client.emit.call_args[0][0]
        assert event.eventType == RunState.RUNNING

    def test_on_end_trace_step_extract_has_input_datasets(
        self, mock_pipeline, mock_extract_info, mock_openlineage_client
    ):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)
        mock_openlineage_client.emit.reset_mock()

        step = MagicMock()
        step.step = "extract"
        step.step_exception = None

        tracker.on_end_trace_step(trace, step, mock_pipeline, mock_extract_info, False)

        event = mock_openlineage_client.emit.call_args[0][0]
        input_names = {d.name for d in event.inputs}
        assert "users" in input_names
        assert "orders" in input_names

    def test_on_end_trace_step_normalize_stores_row_counts(
        self, mock_pipeline, mock_normalize_info, mock_openlineage_client
    ):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)
        mock_openlineage_client.emit.reset_mock()

        step = MagicMock()
        step.step = "normalize"
        step.step_exception = None

        tracker.on_end_trace_step(trace, step, mock_pipeline, mock_normalize_info, False)

        # Normalize doesn't emit an event
        mock_openlineage_client.emit.assert_not_called()
        # But row counts are stored
        assert tracker._normalize_row_counts == {"users": 100, "orders": 250}

    def test_on_end_trace_step_load_emits_complete(
        self, mock_pipeline, mock_load_info, mock_openlineage_client
    ):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)
        mock_openlineage_client.emit.reset_mock()

        step = MagicMock()
        step.step = "load"
        step.step_exception = None

        tracker.on_end_trace_step(trace, step, mock_pipeline, mock_load_info, False)

        mock_openlineage_client.emit.assert_called_once()
        event = mock_openlineage_client.emit.call_args[0][0]
        assert event.eventType == RunState.COMPLETE

    def test_on_end_trace_step_failure_emits_fail(self, mock_pipeline, mock_openlineage_client):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)
        mock_openlineage_client.emit.reset_mock()

        step = MagicMock()
        step.step = "load"
        step.step_exception = "Connection failed"

        tracker.on_end_trace_step(trace, step, mock_pipeline, None, False)

        mock_openlineage_client.emit.assert_called_once()
        event = mock_openlineage_client.emit.call_args[0][0]
        assert event.eventType == RunState.FAIL
        assert "errorMessage" in event.run.facets

    def test_on_end_trace_clears_state(self, mock_pipeline, mock_openlineage_client):
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)

        tracker.on_end_trace(trace, mock_pipeline, False)

        assert tracker._run_id is None
        assert tracker._pipeline_name is None
        assert tracker._normalize_row_counts == {}

    def test_on_end_trace_step_skips_without_run_id(
        self, mock_pipeline, mock_openlineage_client
    ):
        """on_end_trace_step should be a no-op if on_start_trace was not called."""
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        step = MagicMock()
        step.step = "load"
        step.step_exception = None

        tracker.on_end_trace_step(MagicMock(), step, mock_pipeline, MagicMock(), False)

        mock_openlineage_client.emit.assert_not_called()

    def test_on_end_trace_emits_complete_when_no_load_step(self, mock_pipeline, mock_openlineage_client):
        """If pipeline ends without a load step, on_end_trace should emit COMPLETE."""
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)

        # Only extract step, no load
        step = MagicMock()
        step.step = "extract"
        step.step_exception = None
        tracker.on_end_trace_step(trace, step, mock_pipeline, MagicMock(), False)
        mock_openlineage_client.emit.reset_mock()

        tracker.on_end_trace(trace, mock_pipeline, False)

        mock_openlineage_client.emit.assert_called_once()
        event = mock_openlineage_client.emit.call_args[0][0]
        assert event.eventType == RunState.COMPLETE

    def test_on_end_trace_no_double_emit_after_load_complete(self, mock_pipeline, mock_load_info, mock_openlineage_client):
        """on_end_trace should not emit again if COMPLETE was already emitted by load step."""
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)

        step = MagicMock()
        step.step = "load"
        step.step_exception = None
        tracker.on_end_trace_step(trace, step, mock_pipeline, mock_load_info, False)
        mock_openlineage_client.emit.reset_mock()

        tracker.on_end_trace(trace, mock_pipeline, False)
        mock_openlineage_client.emit.assert_not_called()

    def test_complete_event_includes_input_datasets(
        self, mock_pipeline, mock_extract_info, mock_load_info, mock_openlineage_client
    ):
        """COMPLETE events should include input datasets from extract."""
        emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
        tracker = OpenLineageTracker(emitter=emitter)

        trace = MagicMock()
        tracker.on_start_trace(trace, "run", mock_pipeline)

        # Extract step
        extract_step = MagicMock()
        extract_step.step = "extract"
        extract_step.step_exception = None
        tracker.on_end_trace_step(trace, extract_step, mock_pipeline, mock_extract_info, False)

        # Load step
        load_step = MagicMock()
        load_step.step = "load"
        load_step.step_exception = None
        tracker.on_end_trace_step(trace, load_step, mock_pipeline, mock_load_info, False)

        events = [call[0][0] for call in mock_openlineage_client.emit.call_args_list]
        complete_events = [e for e in events if e.eventType == RunState.COMPLETE]
        assert len(complete_events) == 1
        complete = complete_events[0]
        assert len(complete.inputs) > 0
        input_names = {d.name for d in complete.inputs}
        assert "users" in input_names
