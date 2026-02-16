"""Tests for the OpenLineage emitter."""
from unittest.mock import MagicMock, patch

from dlt_openlineage.emitter import OpenLineageEmitter


class TestOpenLineageEmitter:
    def test_api_key_uses_client_options(self):
        """Verify that passing api_key creates OpenLineageClientOptions, not a dict."""
        with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
            mock_cls.return_value = MagicMock()
            emitter = OpenLineageEmitter(url="http://test:5000", api_key="test-key")
            call_kwargs = mock_cls.call_args[1]
            assert hasattr(call_kwargs["options"], "api_key")
            assert call_kwargs["options"].api_key == "test-key"

    def test_emit_swallows_transport_errors(self):
        """Verify that transport failures don't propagate."""
        with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
            mock_client = MagicMock()
            mock_client.emit.side_effect = ConnectionError("unreachable")
            mock_cls.return_value = mock_client
            emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
            # Should not raise
            emitter.emit_start(pipeline_name="test", run_id="00000000-0000-0000-0000-000000000001")
            emitter.emit_running(pipeline_name="test", run_id="00000000-0000-0000-0000-000000000001", step_name="extract")
            emitter.emit_complete(pipeline_name="test", run_id="00000000-0000-0000-0000-000000000001")
            emitter.emit_fail(pipeline_name="test", run_id="00000000-0000-0000-0000-000000000001", error="test error")

    def test_emit_fail_string_error_uses_string_as_stack_trace(self):
        """dlt passes string errors; verify they become the stackTrace."""
        with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
            emitter.emit_fail(
                pipeline_name="test",
                run_id="00000000-0000-0000-0000-000000000001",
                error="Connection timed out\n  at db.connect()",
                step_name="load",
            )
            event = mock_client.emit.call_args[0][0]
            facet = event.run.facets["errorMessage"]
            assert facet.message == "Connection timed out\n  at db.connect()"
            assert facet.stackTrace == "Connection timed out\n  at db.connect()"

    def test_emit_fail_exception_error_captures_traceback(self):
        """Verify real exceptions get their traceback captured."""
        with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            emitter = OpenLineageEmitter(url="http://test:5000", namespace="test")
            try:
                raise ValueError("test exception")
            except ValueError as e:
                emitter.emit_fail(
                    pipeline_name="test",
                    run_id="00000000-0000-0000-0000-000000000001",
                    error=e,
                    step_name="load",
                )
            event = mock_client.emit.call_args[0][0]
            facet = event.run.facets["errorMessage"]
            assert facet.message == "test exception"
            assert "ValueError" in facet.stackTrace
            assert "test exception" in facet.stackTrace
