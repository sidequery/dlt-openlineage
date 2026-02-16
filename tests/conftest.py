"""Shared test fixtures."""
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_pipeline():
    """Create a mock dlt pipeline."""
    pipeline = MagicMock()
    pipeline.pipeline_name = "test_pipeline"

    # Mock schema
    schema = MagicMock()
    schema.tables = {
        "users": {
            "columns": {
                "id": {"data_type": "bigint"},
                "name": {"data_type": "text"},
                "email": {"data_type": "text"},
            }
        },
        "orders": {
            "columns": {
                "id": {"data_type": "bigint"},
                "user_id": {"data_type": "bigint"},
                "amount": {"data_type": "decimal"},
            }
        },
        "_dlt_loads": {
            "columns": {"load_id": {"data_type": "text"}}
        },
    }
    pipeline.default_schema = schema
    return pipeline


@pytest.fixture
def mock_load_info():
    """Create a mock LoadInfo."""
    load_info = MagicMock()
    load_info.destination_type = "duckdb"
    load_info.destination_name = "duckdb"
    load_info.destination_fingerprint = "local"
    load_info.dataset_name = "test_dataset"

    # Mock load packages with completed jobs
    job1 = MagicMock()
    job1.job_file_info.table_name = "users"
    job2 = MagicMock()
    job2.job_file_info.table_name = "orders"
    job3 = MagicMock()
    job3.job_file_info.table_name = "_dlt_loads"

    load_package = MagicMock()
    load_package.jobs = {
        "completed_jobs": [job1, job2, job3],
        "failed_jobs": [],
    }
    load_info.load_packages = [load_package]
    return load_info


@pytest.fixture
def mock_extract_info():
    """Create a mock ExtractInfo.

    ExtractInfo.metrics is Dict[str, List[ExtractMetrics]] where
    ExtractMetrics is a TypedDict with table_metrics, resource_metrics, etc.
    """
    extract_info = MagicMock()
    # ExtractMetrics are TypedDicts (behave like dicts)
    extract_metric = {
        "started_at": None,
        "finished_at": None,
        "schema_name": "test_schema",
        "table_metrics": {
            "users": MagicMock(items_count=100, file_size=1024),
            "orders": MagicMock(items_count=250, file_size=2048),
        },
        "resource_metrics": {
            "users": MagicMock(items_count=100),
            "orders": MagicMock(items_count=250),
        },
        "job_metrics": {},
        "dag": [],
        "hints": {},
    }
    extract_info.metrics = {
        "load_id_1": [extract_metric],
    }
    return extract_info


@pytest.fixture
def mock_normalize_info():
    """Create a mock NormalizeInfo."""
    normalize_info = MagicMock()
    normalize_info.row_counts = {"users": 100, "orders": 250}
    return normalize_info


@pytest.fixture
def mock_openlineage_client():
    """Patch the OpenLineageClient."""
    with patch("dlt_openlineage.emitter.OpenLineageClient") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        yield mock_client
