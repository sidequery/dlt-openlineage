"""Installation entry point for dlt-openlineage."""
from __future__ import annotations

import os
import logging
import threading
import typing as t

logger = logging.getLogger(__name__)

_installed = False
_install_lock = threading.Lock()


def install(
    url: t.Optional[str] = None,
    namespace: str = "dlt",
    api_key: t.Optional[str] = None,
) -> None:
    """Install the OpenLineage tracker into dlt's tracking system.

    Args:
        url: OpenLineage endpoint URL. Falls back to OPENLINEAGE_URL env var.
            Use "console://" for stdout output.
        namespace: Job namespace. Falls back to OPENLINEAGE_NAMESPACE env var.
        api_key: API key for HTTP transport. Falls back to OPENLINEAGE_API_KEY env var.
    """
    global _installed

    with _install_lock:
        if _installed:
            return

        url = url if url is not None else os.environ.get("OPENLINEAGE_URL")
        namespace = namespace if namespace is not None else os.environ.get("OPENLINEAGE_NAMESPACE", "dlt")
        api_key = api_key if api_key is not None else os.environ.get("OPENLINEAGE_API_KEY")

        if not url:
            raise ValueError(
                "OpenLineage URL is required. Pass url= or set OPENLINEAGE_URL environment variable."
            )

        from dlt_openlineage.emitter import OpenLineageEmitter
        from dlt_openlineage.tracker import OpenLineageTracker

        emitter = OpenLineageEmitter(url=url, namespace=namespace, api_key=api_key)
        tracker = OpenLineageTracker(emitter=emitter)

        # Register with dlt's tracking system
        from dlt.pipeline.trace import TRACKING_MODULES

        TRACKING_MODULES.append(tracker)

        _installed = True
        logger.info("dlt-openlineage installed (url=%s, namespace=%s)", url, namespace)


def is_installed() -> bool:
    """Check if dlt-openlineage is installed."""
    return _installed
