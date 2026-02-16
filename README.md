# dlt-openlineage

OpenLineage integration for [dlt](https://dlthub.com/) pipelines. Automatically emits lineage events to [Marquez](https://marquezproject.ai/) or any OpenLineage-compatible backend.

## Features

- **Table-level lineage**: Track which resources a pipeline extracts and which destination tables it writes (no column-level lineage, see [reshaping notes](#important-dlt-reshaping-and-what-that-means-for-lineage))
- **Schema capture**: Column names and dlt data types for each destination table
- **Row counts**: Per-table row counts from the normalize step
- **Destination-aware namespaces**: Output datasets namespaced by destination type and fingerprint
- **Per-step events**: START, RUNNING (extract), COMPLETE (load), FAIL events
- **Processing engine metadata**: dlt version and adapter version in every event

## Installation

```bash
pip install dlt-openlineage
```

Or with uv:

```bash
uv add dlt-openlineage
```

## Quick Start

Add two lines before your pipeline runs:

```python
import dlt
import dlt_openlineage

dlt_openlineage.install(url="http://localhost:5000")

@dlt.resource
def users():
    yield [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="duckdb",
    dataset_name="raw_data",
)

pipeline.run(users())
```

That's it. OpenLineage events are emitted automatically during pipeline execution.

## Environment Variables

You can also configure via environment variables:

```bash
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=my_project
export OPENLINEAGE_API_KEY=...  # optional, for authenticated endpoints
```

Then:

```python
import dlt_openlineage
dlt_openlineage.install()  # reads from env vars
```

## How It Works

This package implements dlt's `SupportsTracking` protocol and registers via `dlt.pipeline.trace.TRACKING_MODULES`. The tracker intercepts pipeline lifecycle events and emits corresponding OpenLineage events:

| dlt Step | OpenLineage Event | Data Included |
|----------|-------------------|---------------|
| Pipeline start | RunEvent(START) | Job type, processing engine |
| Extract complete | RunEvent(RUNNING) | Input datasets (extracted tables) |
| Load complete | RunEvent(COMPLETE) | Output datasets with schema, row counts, input datasets |
| Any step failure | RunEvent(FAIL) | Error message and stack trace |
| Pipeline end (no load) | RunEvent(COMPLETE) | Fallback terminal event from pipeline schema |

## What Gets Emitted

### Input datasets

Input datasets are derived from dlt's `ExtractInfo.metrics`, which tracks which tables/resources were extracted. These are the table names as dlt sees them after extraction (i.e., the resource names or table names from `table_metrics`). Internal dlt tables (`_dlt_loads`, `_dlt_version`, etc.) are filtered out.

Input datasets appear in both the RUNNING event (after extract) and the COMPLETE event (after load), so lineage consumers can see the full input-to-output picture on the terminal event.

Inputs are namespaced with the configured namespace (defaults to `"dlt"`), not the destination, since they represent the logical source data before loading.

### Output datasets

Output datasets are built from `LoadInfo.load_packages`, which lists every completed load job with its target table name. Each output dataset includes:

- **Namespace**: derived from the destination type and fingerprint, e.g. `duckdb://local`, `postgres://myhost.com`, `bigquery://project-id`
- **Name**: qualified as `{dataset_name}.{table_name}`, e.g. `raw_data.users`, `raw_data.orders`
- **Schema facet**: column names and dlt data types (e.g. `bigint`, `text`, `decimal`) from the pipeline's default schema
- **Row counts**: per-table row counts from the normalize step's `NormalizeInfo.row_counts`

If `LoadInfo` isn't available (e.g. extract-only runs), output datasets fall back to the pipeline's `default_schema.tables`.

### Important: dlt reshaping and what that means for lineage

dlt aggressively reshapes data between extract and load. This affects what shows up in lineage:

- **Nested data is flattened**: dlt unnests JSON objects and arrays into separate tables. A resource `users` with a nested `addresses` array becomes two destination tables: `users` and `users__addresses`. Both appear as output datasets. The input side just shows `users` (the original resource name).
- **Column names are normalized**: dlt converts column names to snake_case and applies naming conventions. The schema facet reflects the normalized names as they exist in the destination, not the original source field names.
- **Column types are dlt types**: schema facets use dlt's internal type system (`bigint`, `text`, `double`, `complex`, `date`, `timestamp`, `wei`, etc.), not the destination's native SQL types.
- **No column-level lineage**: because dlt's reshaping (flattening, renaming, type coercion) isn't tracked as a transformation DAG, we emit table-level lineage only. There is no `ColumnLineageDatasetFacet`. This is an honest representation: we can tell you that `users` resource produced the `raw_data.users` and `raw_data.users__addresses` tables, but we can't trace individual columns through dlt's normalizer.
- **Internal tables are excluded**: dlt creates `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`, and similar bookkeeping tables. These are filtered from both input and output datasets.

### Run and job facets

Every event includes:

- **`jobType`**: `{processingType: "BATCH", integration: "DLT", jobType: "PIPELINE"}`
- **`processing_engine`**: dlt version and adapter version
- **`dlt_execution`** (custom facet, on COMPLETE): current step, destination type/name, dataset name, total/failed job counts
- **`errorMessage`** (on FAIL): error message, programming language, stack trace (the string representation dlt provides)

## Testing with Marquez

```bash
# Start Marquez locally
docker run -p 5000:5000 -p 5001:5001 -p 3000:3000 marquezproject/marquez

# Point your pipeline at it
export OPENLINEAGE_URL=http://localhost:5000

# Run your dlt pipeline
python my_pipeline.py

# View lineage at http://localhost:3000
```

## Development

```bash
# Install dependencies
uv sync --dev

# Run tests (unit + integration with real dlt+DuckDB)
uv run pytest tests/ -v

# Lint
uv run ruff check src/
```

## License

MIT
