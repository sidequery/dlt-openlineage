# dlt-openlineage

OpenLineage integration for [dlt](https://dlthub.com/) pipelines. Automatically emits lineage events to [Marquez](https://marquezproject.ai/) or any OpenLineage-compatible backend.

## Features

- **Input/output dataset lineage**: Track which tables a pipeline reads and writes
- **Schema capture**: Column names and types for each destination table
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

Output dataset namespaces are derived from the destination (e.g., `duckdb://local`, `postgres://host:5432`), and dataset names are qualified with the dataset name (e.g., `raw_data.users`).

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
