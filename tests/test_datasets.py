"""Tests for dataset conversion functions."""
from dlt_openlineage.datasets import (
    extract_info_to_input_datasets,
    load_info_to_output_datasets,
    pipeline_to_output_datasets_from_schema,
    _schema_to_facet,
)


class TestDatasetConversion:
    def test_extract_info_to_input_datasets(self, mock_extract_info):
        inputs = extract_info_to_input_datasets(mock_extract_info, "test")
        names = {d.name for d in inputs}
        assert "users" in names
        assert "orders" in names
        # Internal tables should be excluded
        assert not any(d.name.startswith("_dlt") for d in inputs)

    def test_extract_info_deduplicates(self, mock_extract_info):
        inputs = extract_info_to_input_datasets(mock_extract_info, "test")
        names = [d.name for d in inputs]
        assert len(names) == len(set(names))

    def test_load_info_to_output_datasets(self, mock_load_info, mock_pipeline):
        row_counts = {"users": 100, "orders": 250}
        outputs = load_info_to_output_datasets(
            mock_load_info, "test", pipeline=mock_pipeline, row_counts=row_counts
        )

        names = {d.name for d in outputs}
        assert "test_dataset.users" in names
        assert "test_dataset.orders" in names
        # Internal tables should be excluded
        assert not any("_dlt" in d.name for d in outputs)

    def test_output_datasets_have_schema_facet(self, mock_load_info, mock_pipeline):
        outputs = load_info_to_output_datasets(
            mock_load_info, "test", pipeline=mock_pipeline
        )

        users_ds = next(d for d in outputs if "users" in d.name)
        assert "schema" in users_ds.facets
        fields = users_ds.facets["schema"].fields
        field_names = {f.name for f in fields}
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names

    def test_output_datasets_have_row_counts(self, mock_load_info, mock_pipeline):
        row_counts = {"users": 100, "orders": 250}
        outputs = load_info_to_output_datasets(
            mock_load_info, "test", pipeline=mock_pipeline, row_counts=row_counts
        )

        users_ds = next(d for d in outputs if "users" in d.name)
        assert "outputStatistics" in users_ds.outputFacets
        assert users_ds.outputFacets["outputStatistics"].rowCount == 100

    def test_output_datasets_namespace_from_destination(self, mock_load_info, mock_pipeline):
        outputs = load_info_to_output_datasets(
            mock_load_info, "test", pipeline=mock_pipeline
        )
        assert len(outputs) > 0
        # Namespace should come from destination info
        assert outputs[0].namespace == "duckdb://local"

    def test_schema_to_facet(self, mock_pipeline):
        facet = _schema_to_facet(mock_pipeline, "users")
        assert facet is not None
        assert len(facet.fields) == 3

        field_names = {f.name for f in facet.fields}
        assert field_names == {"id", "name", "email"}

    def test_schema_to_facet_missing_table(self, mock_pipeline):
        facet = _schema_to_facet(mock_pipeline, "nonexistent")
        assert facet is None

    def test_dlt_internal_tables_excluded(self, mock_load_info, mock_pipeline):
        outputs = load_info_to_output_datasets(
            mock_load_info, "test", pipeline=mock_pipeline
        )
        for ds in outputs:
            assert "_dlt" not in ds.name

    def test_pipeline_to_output_datasets_from_schema(self, mock_pipeline):
        outputs = pipeline_to_output_datasets_from_schema(mock_pipeline, "test")
        names = {d.name for d in outputs}
        assert "users" in names
        assert "orders" in names
        assert "_dlt_loads" not in names

    def test_pipeline_to_output_datasets_with_row_counts(self, mock_pipeline):
        row_counts = {"users": 50}
        outputs = pipeline_to_output_datasets_from_schema(
            mock_pipeline, "test", row_counts=row_counts
        )
        users_ds = next(d for d in outputs if d.name == "users")
        assert "outputStatistics" in users_ds.outputFacets
        assert users_ds.outputFacets["outputStatistics"].rowCount == 50
