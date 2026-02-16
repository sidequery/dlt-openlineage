"""dlt-openlineage: OpenLineage integration for dlt pipelines.

Usage:
    # In your pipeline script, before running:
    import dlt_openlineage
    dlt_openlineage.install(url="http://localhost:5000")

    # Or via environment variables:
    # OPENLINEAGE_URL=http://localhost:5000
    # OPENLINEAGE_NAMESPACE=my_project
    import dlt_openlineage
    dlt_openlineage.install()
"""

__version__ = "0.1.0"

from dlt_openlineage.install import install, is_installed
from dlt_openlineage.tracker import OpenLineageTracker
from dlt_openlineage.emitter import OpenLineageEmitter

__all__ = ["install", "is_installed", "OpenLineageTracker", "OpenLineageEmitter"]
