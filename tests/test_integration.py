"""Integration test with a real dlt pipeline."""
import sys

from unittest.mock import MagicMock, patch

from openlineage.client.event_v2 import RunState

import dlt_openlineage


def _reset_install_state():
    """Reset the install module's _installed flag and clean up TRACKING_MODULES."""
    # Access the actual module (not the function re-exported on the package)
    install_mod = sys.modules["dlt_openlineage.install"]
    install_mod._installed = False

    from dlt.pipeline.trace import TRACKING_MODULES

    TRACKING_MODULES[:] = [
        m
        for m in TRACKING_MODULES
        if not isinstance(m, dlt_openlineage.OpenLineageTracker)
    ]


class TestDltOpenLineageIntegration:
    """Integration tests using real dlt with DuckDB destination."""

    def test_full_pipeline_emits_events(self, tmp_path):
        """Run a real dlt pipeline and verify OpenLineage events are emitted."""
        import dlt
        import dlt_openlineage

        _reset_install_state()

        # Capture emitted events
        emitted_events = []

        with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
            mock_client = MagicMock()
            mock_client.emit = lambda event: emitted_events.append(event)
            mock_cls.return_value = mock_client

            dlt_openlineage.install(url="http://test:5000", namespace="test")

            # Create a simple dlt pipeline
            @dlt.resource
            def users():
                yield [
                    {"id": 1, "name": "Alice", "email": "alice@example.com"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com"},
                ]

            @dlt.resource
            def orders():
                yield [
                    {"id": 1, "user_id": 1, "amount": 99.99},
                    {"id": 2, "user_id": 2, "amount": 149.99},
                ]

            pipeline = dlt.pipeline(
                pipeline_name="test_integration_pipeline",
                destination="duckdb",
                dataset_name="test_data",
                pipelines_dir=str(tmp_path),
            )

            pipeline.run([users(), orders()])

        _reset_install_state()

        # Verify events
        assert len(emitted_events) >= 2, (
            f"Expected at least 2 events (START + COMPLETE), got {len(emitted_events)}: "
            f"{[e.eventType for e in emitted_events]}"
        )

        # Check START event
        start_events = [e for e in emitted_events if e.eventType == RunState.START]
        assert len(start_events) == 1
        assert start_events[0].job.name == "test_integration_pipeline"

        # Check COMPLETE event
        complete_events = [e for e in emitted_events if e.eventType == RunState.COMPLETE]
        assert len(complete_events) == 1

        # Complete should have output datasets
        complete = complete_events[0]
        assert len(complete.outputs) > 0, "COMPLETE event should have output datasets"
        output_names = {d.name for d in complete.outputs}
        assert any("users" in name for name in output_names)
        assert any("orders" in name for name in output_names)

    def test_full_pipeline_running_event_has_inputs(self, tmp_path):
        """Verify the RUNNING event from extract has input datasets."""
        import dlt
        import dlt_openlineage

        _reset_install_state()

        emitted_events = []

        with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
            mock_client = MagicMock()
            mock_client.emit = lambda event: emitted_events.append(event)
            mock_cls.return_value = mock_client

            dlt_openlineage.install(url="http://test:5000", namespace="test")

            @dlt.resource
            def users():
                yield [{"id": 1, "name": "Alice"}]

            pipeline = dlt.pipeline(
                pipeline_name="test_running_pipeline",
                destination="duckdb",
                dataset_name="test_data",
                pipelines_dir=str(tmp_path),
            )

            pipeline.run(users())

        _reset_install_state()

        # Check RUNNING event (from extract step)
        running_events = [e for e in emitted_events if e.eventType == RunState.RUNNING]
        assert len(running_events) >= 1

        running = running_events[0]
        assert len(running.inputs) > 0, "RUNNING event should have input datasets"
        input_names = {d.name for d in running.inputs}
        assert "users" in input_names
