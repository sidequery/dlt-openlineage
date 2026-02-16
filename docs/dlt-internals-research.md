# dlt Internals Research: Extension Points for OpenLineage Integration

Research into the dlt (data load tool) library's internals to identify hooks, events,
callbacks, metadata, and extension points for emitting OpenLineage events.

**Repository**: https://github.com/dlt-hub/dlt
**Documentation**: https://dlthub.com/docs

---

## 1. Pipeline Lifecycle

A dlt pipeline goes through three sequential phases when `pipeline.run()` is called:

```
extract -> normalize -> load
```

### Phase Details

| Phase | Purpose | Input | Output |
|-------|---------|-------|--------|
| **Extract** | Pull data from sources to local disk | DltSource, generators, iterables | Load package with raw extracted files |
| **Normalize** | Infer/apply schema, restructure data | Extracted load package | Normalized load package (jsonl/parquet) |
| **Load** | Transfer data to destination | Normalized load package | Data in destination tables |

### `pipeline.run()` Flow

1. Optionally syncs state with destination (`restore_from_destination`)
2. Checks for pending data from previous runs (normalizes/loads it first)
3. Calls `self.extract(data, ...)`
4. Calls `self.normalize()`
5. Calls `self.load(destination, dataset_name, credentials=credentials)`
6. Returns `LoadInfo`

Each phase can also be called independently: `pipeline.extract()`, `pipeline.normalize()`,
`pipeline.load()`.

### Pipeline Step Types

Defined in `dlt/pipeline/typing.py`:

```python
TPipelineStep = Literal["run", "sync", "extract", "normalize", "load"]
```

---

## 2. Events/Signals/Hooks: The `SupportsTracking` Protocol

**This is the primary extension point for OpenLineage integration.**

### The `SupportsTracking` Protocol

Defined in `dlt/common/runtime/tracking.py`:

```python
class SupportsTracking(Protocol):
    def on_start_trace(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None: ...

    def on_start_trace_step(
        self, trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
    ) -> None: ...

    def on_end_trace_step(
        self,
        trace: PipelineTrace,
        step: PipelineStepTrace,
        pipeline: SupportsPipeline,
        step_info: Any,
        send_state: bool,
    ) -> None: ...

    def on_end_trace(
        self, trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool
    ) -> None: ...
```

### How Tracking Modules Are Registered

In `dlt/pipeline/__init__.py`:

```python
from dlt.pipeline import trace, track, platform

trace.TRACKING_MODULES = [track, platform]
```

The `TRACKING_MODULES` list in `dlt/pipeline/trace.py` is a module-level list:

```python
TRACKING_MODULES: List[SupportsTracking] = None
```

**To add a custom tracker, you append to this list**:

```python
from dlt.pipeline import trace
trace.TRACKING_MODULES.append(my_openlineage_tracker)
```

The workspace context also demonstrates this pattern (`dlt/_workspace/_workspace_context.py`):

```python
if runtime_artifacts not in trace.TRACKING_MODULES:
    trace.TRACKING_MODULES.append(runtime_artifacts)
```

### When Hooks Fire

All hooks are called from `dlt/pipeline/trace.py`. The `with_runtime_trace` decorator
wraps `extract`, `normalize`, `load`, and `run` methods:

```
on_start_trace(trace, step, pipeline)       # Once when a new trace begins
  on_start_trace_step(trace, step, pipeline)  # Before each step (extract/normalize/load)
  # ... step executes ...
  on_end_trace_step(trace, step, pipeline, step_info, send_state)  # After each step
on_end_trace(trace, pipeline, send_state)   # Once when trace completes
```

When `pipeline.run()` is called, it creates a single trace. The inner `extract()`,
`normalize()`, and `load()` calls each create trace steps within that trace. When
these methods are called individually, each creates its own trace with a single step,
and traces are merged via `merge_traces()`.

### Error Handling in Hooks

All hook calls are wrapped with `suppress_and_warn()`:

```python
for module in TRACKING_MODULES:
    with suppress_and_warn(f"on_start_trace on module {module} failed"):
        module.on_start_trace(trace, step, pipeline)
```

This means **a failing tracking module will not crash the pipeline**. Errors are logged
as warnings but execution continues.

### Existing Tracking Implementations

Two built-in modules implement `SupportsTracking`:

1. **`dlt/pipeline/track.py`**: Sends telemetry to Segment and creates Sentry transactions/spans
2. **`dlt/pipeline/platform.py`**: Sends traces and schema state to the dlthub platform

Both serve as reference implementations for building an OpenLineage tracker.

---

## 3. Pipeline Object Properties

The `Pipeline` class (in `dlt/pipeline/pipeline.py`) implements `SupportsPipeline` protocol.

### Key Properties Available on Pipeline

| Property | Type | Description |
|----------|------|-------------|
| `pipeline_name` | `str` | Name identifier for the pipeline |
| `destination` | `AnyDestination` | Destination reference (has `.destination_name`, `.destination_type`) |
| `dataset_name` | `str` | Target dataset/schema name |
| `default_schema_name` | `str` | Default schema name for the pipeline |
| `working_dir` | `str` | Pipeline's working directory path |
| `pipeline_salt` | `str` | Configurable encryption seed |
| `first_run` | `bool` | Whether this is the first successful run |
| `schemas` | `Mapping[str, Schema]` | All known schemas |
| `state` | `TPipelineState` | Current pipeline state dictionary |
| `has_pending_data` | `bool` | Whether unprocessed packages exist |
| `collector` | `Collector` | Progress tracking component |
| `run_context` | `RunContextBase` | Run context with runtime config |
| `last_run_context` | `TLastRunContext` | Context from last successful run |

### Pipeline State Structure (`TPipelineState`)

```python
class TPipelineState(TVersionedState, total=False):
    pipeline_name: str
    dataset_name: str
    default_schema_name: Optional[str]
    schema_names: Optional[List[str]]
    destination_name: Optional[str]
    destination_type: Optional[str]
    staging_name: Optional[str]
    staging_type: Optional[str]
    _local: TPipelineLocalState
    sources: NotRequired[Dict[str, Dict[str, Any]]]
```

---

## 4. Source and Resource Metadata

### DltSource (`dlt/extract/source.py`)

| Property | Description |
|----------|-------------|
| `name` | Source name (derived from schema) |
| `schema` | The `Schema` object for this source |
| `resources` | `DltResourceDict` of all resources |
| `selected_resources` | Dictionary of resources selected for extraction |
| `section` | Config section identifier |
| `max_table_nesting` | Max depth of nested tables |
| `root_key` | Whether FK propagation to nested tables is enabled |
| `schema_contract` | Schema validation settings |
| `exhausted` | Whether the iterator has been consumed |
| `state` | Source-scoped state from active pipeline |

### DltResource (`dlt/extract/resource.py`)

| Property | Description |
|----------|-------------|
| `name` | Resource identifier |
| `source_name` | Name of parent source |
| `is_transformer` | Whether resource receives data from another resource |
| `incremental` | Incremental transform configuration |
| `selected` | Whether resource is selected for extraction |
| `state` | Resource-scoped state from active pipeline |

### Resource Table Hints (via `_hints` dictionary)

```python
table_name: str           # Target table name
primary_key: TColumnNames # Primary key columns
merge_key: TColumnNames   # Merge key columns
columns: dict             # Column definitions
write_disposition: str    # append/replace/merge/skip
parent_table_name: str    # Parent table (hierarchical)
references: list          # Foreign key relationships
schema_contract: dict     # Schema validation rules
table_format: str         # iceberg/delta/hive/native
file_format: str          # File encoding spec
```

---

## 5. Load Info (Return Value of `pipeline.run()` and `pipeline.load()`)

### `LoadInfo` (`dlt/common/pipeline.py`)

```python
class LoadInfo(StepInfo[LoadMetrics]):
    pipeline: SupportsPipeline
    metrics: Dict[str, List[LoadMetrics]]          # Metrics per load_id
    destination_type: str                           # e.g. "dlt.destinations.bigquery"
    destination_displayable_credentials: str        # Obfuscated connection string
    destination_name: str                           # e.g. "bigquery"
    environment: str
    staging_type: str                               # Staging destination type
    staging_name: str                               # Staging destination name
    staging_displayable_credentials: str
    destination_fingerprint: str                    # Hash of destination config
    dataset_name: str                               # Target dataset
    loads_ids: List[str]                            # Load package IDs
    load_packages: List[LoadPackageInfo]            # Detailed package info
    first_run: bool
```

**Key properties**:
- `has_failed_jobs` -> `bool`: Whether any jobs failed
- `raise_on_failed_jobs()`: Raises `DestinationHasFailedJobs` if failures exist
- `started_at` / `finished_at`: Timestamps
- `is_empty`: Whether any packages were processed

### `LoadMetrics`

```python
class LoadMetrics(StepMetrics):
    job_metrics: Dict[str, LoadJobMetrics]

class LoadJobMetrics(NamedTuple):
    job_id: str
    file_path: str
    table_name: str
    started_at: datetime.datetime
    finished_at: Optional[datetime.datetime]
    state: str
    remote_url: Optional[str]
    retry_count: int = 0
```

### `ExtractInfo`

```python
class ExtractInfo(StepInfo[ExtractMetrics]):
    pipeline: SupportsPipeline
    metrics: Dict[str, List[ExtractMetrics]]
    extract_data_info: List[ExtractDataInfo]
    loads_ids: List[str]
    load_packages: List[LoadPackageInfo]
    first_run: bool
```

### `ExtractMetrics`

```python
class ExtractMetrics(StepMetrics):
    schema_name: str
    job_metrics: Dict[str, DataWriterMetrics]       # Per job file
    table_metrics: Dict[str, DataWriterMetrics]     # Aggregated by table
    resource_metrics: Dict[str, DataWriterAndCustomMetrics]  # Aggregated by resource
    dag: List[Tuple[str, str]]                      # Resource dependency graph edges
    hints: Dict[str, Dict[str, Any]]                # Resource hints
```

### `NormalizeInfo`

```python
class NormalizeInfo(StepInfo[NormalizeMetrics]):
    row_counts: RowCounts  # Dict[str, int] table_name -> row_count
```

### `DataWriterMetrics`

```python
class DataWriterMetrics(NamedTuple):
    file_path: str
    items_count: int
    file_size: int
    created: float
    last_modified: float
```

---

## 6. Schema System

### Schema Class (`dlt/common/schema/schema.py`)

Key properties:
- `name`: Schema name
- `tables` -> `TSchemaTables` (Dict[str, TTableSchema]): All table schemas
- `data_tables(seen_data_only=False)`: List of user data tables (excludes dlt system tables)
- `dlt_tables()`: List of dlt system tables
- `version` / `stored_version`: Schema version numbers
- `version_hash` / `stored_version_hash`: Content hashes
- `is_modified`: Whether schema changed since loading
- `is_new`: Whether schema was ever saved
- `references`: Foreign key references between tables

### Table Schema (`TTableSchema`)

```python
class TTableSchema(TypedDict, total=False):
    name: Optional[str]
    description: Optional[str]
    columns: TTableSchemaColumns              # Dict[str, TColumnSchema]
    write_disposition: Optional[TWriteDisposition]  # append/replace/merge/skip
    references: Optional[TTableReferenceParam]
    schema_contract: Optional[TSchemaContract]
    table_sealed: Optional[bool]
    parent: Optional[str]                     # Parent table name (nested tables)
    resource: Optional[str]                   # Source resource name
    table_format: Optional[TTableFormat]      # iceberg/delta/hive/native
    file_format: Optional[TFileFormat]
    filters: Optional[TRowFilters]
```

### Column Schema (`TColumnSchema`)

```python
class TColumnSchema(TypedDict, total=False):
    name: Optional[str]
    description: Optional[str]
    data_type: Optional[TDataType]
    nullable: Optional[bool]
    precision: Optional[int]
    scale: Optional[int]
    timezone: Optional[bool]
    # Hints:
    primary_key: Optional[bool]
    unique: Optional[bool]
    merge_key: Optional[bool]
    sort: Optional[bool]
    partition: Optional[bool]
    cluster: Optional[bool]
    row_key: Optional[bool]          # _dlt_id
    parent_key: Optional[bool]       # _dlt_parent_id
    root_key: Optional[bool]         # _dlt_root_id
    variant: Optional[bool]
    hard_delete: Optional[bool]
    dedup_sort: Optional[TSortOrder]
    incremental: Optional[bool]
```

### Data Types

```python
TDataType = Literal[
    "text", "double", "bool", "timestamp", "bigint",
    "binary", "json", "decimal", "wei", "date", "time"
]
```

### Write Dispositions and Merge Strategies

```python
TWriteDisposition = Literal["skip", "append", "replace", "merge"]
TLoaderMergeStrategy = Literal["delete-insert", "scd2", "upsert"]
TLoaderReplaceStrategy = Literal["truncate-and-insert", "insert-from-staging", "staging-optimized"]
```

### Table References (Foreign Keys)

```python
class _TTableReferenceBase(TypedDict, total=False):
    label: Optional[str]                              # Semantic label
    cardinality: Optional[TReferenceCardinality]      # e.g. "one_to_many"
    columns: Sequence[str]                            # Column(s) in this table
    referenced_table: str                             # Target table
    referenced_columns: Sequence[str]                 # Target column(s)
```

### System Tables (prefixed with `_dlt_`)

- `_dlt_version`: Schema version tracking
- `_dlt_loads`: Load package tracking
- `_dlt_pipeline_state`: Pipeline state storage

Each row has a `_dlt_id` (unique row key). Nested tables additionally have
`_dlt_parent_id` and `_dlt_list_idx`.

---

## 7. Destination Info

### Supported Destinations

BigQuery, Snowflake, Postgres, Redshift, DuckDB, MotherDuck, Databricks, ClickHouse,
Microsoft SQL Server, Azure Synapse, AWS Athena/Glue, SQLAlchemy (30+ databases),
Filesystem (S3/GCS/Azure Blob), Delta, Iceberg, Weaviate, LanceDB, Qdrant, Dremio,
Custom destination (sink functions).

### Destination Configuration Hierarchy

```
DestinationClientConfiguration                    # Base: destination_type, destination_name
  DestinationClientDwhConfiguration               # + dataset_name, default_schema_name
    DestinationClientDwhWithStagingConfiguration   # + staging support
  DestinationClientStagingConfiguration            # + bucket_url for file staging
```

### Accessing Destination Info

```python
# From pipeline
pipeline.destination.destination_name     # e.g. "bigquery"
pipeline.destination.destination_type     # e.g. "dlt.destinations.bigquery"

# From LoadInfo
load_info.destination_type
load_info.destination_name
load_info.destination_displayable_credentials  # Obfuscated credentials
load_info.destination_fingerprint              # Hash of connection config
load_info.staging_type
load_info.staging_name
load_info.dataset_name

# Direct client access
client = pipeline.destination_client(schema_name)
# client.config contains DestinationClientConfiguration
# client.config.fingerprint() returns hash of selected config fields
```

### Destination Capabilities

Accessible via `pipeline.destination.capabilities()`:
- Supported loader file formats
- Max identifier lengths
- Naming convention support
- Case sensitivity handling

---

## 8. Tracing/Telemetry System

### Trace Architecture

The tracing system is the backbone for the tracking hooks.

**`PipelineTrace`** (overall trace):
```python
class PipelineTrace(NamedTuple):
    transaction_id: str
    pipeline_name: str
    execution_context: TExecutionContext
    started_at: datetime.datetime
    steps: List[PipelineStepTrace]
    finished_at: datetime.datetime
    resolved_config_values: List[SerializableResolvedValueTrace]
    engine_version: int
```

**`PipelineStepTrace`** (per-step trace):
```python
class PipelineStepTrace(NamedTuple):
    span_id: str
    step: TPipelineStep                    # "extract", "normalize", "load", "run"
    started_at: datetime.datetime
    finished_at: datetime.datetime
    step_info: Optional[StepInfo]          # ExtractInfo, NormalizeInfo, or LoadInfo
    step_exception: Optional[str]          # Exception message if failed
    exception_traces: List[ExceptionTrace] # Full exception chain
```

### The `with_runtime_trace` Decorator

Defined in `dlt/pipeline/pipeline.py`:

```python
def with_runtime_trace(send_state: bool = False) -> Callable[[TFun], TFun]:
```

Applied to all pipeline step methods:

```python
@with_runtime_trace()           # extract
@with_runtime_trace()           # normalize
@with_runtime_trace(send_state=True)  # load (sends state)
@with_runtime_trace()           # run
```

The decorator:
1. Creates a new `PipelineTrace` if none exists (`start_trace`)
2. Creates a `PipelineStepTrace` (`start_trace_step`)
3. Executes the wrapped method
4. Finalizes the step trace (`end_trace_step`)
5. If this was the outermost trace, finalizes and saves (`end_trace`, `save_trace`)
6. Single-step traces are merged with previous trace via `merge_traces()`

### Accessing the Trace

```python
pipeline.last_trace                          # PipelineTrace from most recent execution
pipeline.last_trace.last_extract_info        # ExtractInfo
pipeline.last_trace.last_normalize_info      # NormalizeInfo
pipeline.last_trace.last_load_info           # LoadInfo
pipeline.last_trace.steps                    # List of all step traces
```

### Built-in Telemetry

1. **Segment/Anonymous telemetry**: Tracks pipeline step completion with hashed identifiers
2. **Sentry integration**: Creates transactions and spans for pipeline steps
3. **Platform sync**: Sends full trace JSON and schema state to dlthub platform
4. **Slack notifications**: Disabled by default, sends load success messages

Configuration in `config.toml`:
```toml
[runtime]
sentry_dsn = "https://..."
dlthub_telemetry = true
dlthub_dsn = "..."
slack_incoming_hook = "..."
```

---

## 9. Plugin/Extension System

### Formal Plugin System

dlt does **not** have a formal plugin system with registration, discovery, or lifecycle
management. However, the `TRACKING_MODULES` mechanism provides an informal but effective
extension point.

### Extension Points Available

1. **`TRACKING_MODULES` list** (primary): Append modules implementing `SupportsTracking`
   to `dlt.pipeline.trace.TRACKING_MODULES`. This is the cleanest extension point.

2. **`Collector` base class**: The pipeline's `collector` also receives trace events
   (`on_start_trace`, `on_start_trace_step`, `on_end_trace_step`, `on_end_trace`).
   Custom collectors could emit OpenLineage events.

3. **Monkey patching**: The `Pipeline` methods (`extract`, `normalize`, `load`, `run`)
   could theoretically be wrapped, but the `TRACKING_MODULES` approach is cleaner.

4. **Post-run inspection**: After `pipeline.run()`, all information is available via
   `LoadInfo`, `pipeline.last_trace`, and `pipeline.schemas`.

### GitHub Issue #63: "Customize pipeline with hooks"

An open issue (https://github.com/dlt-hub/dlt/issues/63) proposes adding formal hooks
for events like load job failure, schema inference, type variance, etc. This has not been
implemented as of the current codebase. A draft PR (#837) explored a "Callback Plugins
Framework" but was not merged.

### How to Implement an OpenLineage Tracker

The recommended approach:

```python
from dlt.pipeline import trace
from dlt.common.runtime.tracking import SupportsTracking

class OpenLineageTracker:
    """Implements SupportsTracking protocol for OpenLineage event emission."""

    def on_start_trace(self, trace, step, pipeline):
        # Emit RunEvent(START) for the pipeline run
        pass

    def on_start_trace_step(self, trace, step, pipeline):
        # Emit RunEvent(START) for extract/normalize/load step
        pass

    def on_end_trace_step(self, trace, step, pipeline, step_info, send_state):
        # Emit RunEvent(COMPLETE/FAIL) with facets from step_info
        # step_info is ExtractInfo, NormalizeInfo, or LoadInfo
        # step_info may be None on error
        pass

    def on_end_trace(self, trace, pipeline, send_state):
        # Emit final RunEvent(COMPLETE/FAIL) for the overall run
        pass

# Register the tracker
tracker = OpenLineageTracker()
trace.TRACKING_MODULES.append(tracker)
```

---

## 10. Error Handling

### Exception Hierarchy

| Exception | When | Contains |
|-----------|------|----------|
| `PipelineStepFailed` | Any extract/normalize/load failure | `pipeline`, `step`, `load_id`, `exception`, `step_info` |
| `LoadClientJobFailed` | Terminal load job failure | Job details, error message |
| `DestinationHasFailedJobs` | Called via `load_info.raise_on_failed_jobs()` | Failed job list |
| `PipelineConfigMissing` | Required config not provided | Config element, step name |
| `CannotRestorePipelineException` | Pipeline state restore failure | Reason string |
| `PipelineHasPendingDataException` | Operation blocked by pending data | Pipeline name, dir |
| `TerminalException` | Marker for non-retryable errors | Varies |

### `PipelineStepFailed` Details

```python
class PipelineStepFailed(PipelineException):
    pipeline: SupportsPipeline
    step: TPipelineStep                    # "extract", "normalize", "load"
    load_id: str                           # Package ID being processed
    exception: BaseException               # Original exception
    step_info: StepInfo                    # Partial step info up to failure
    has_pending_data: bool
    is_package_partially_loaded: bool      # Whether some jobs completed before failure
```

### How Errors Appear in Traces

In `end_trace_step`, if `step_info` is an exception:
- `step_exception` is set to the exception message string
- `exception_traces` contains the full exception chain via `get_exception_trace_chain()`
- Each trace in the chain includes: `pipeline_name`, `source_name`, `resource_name`

### Load Package Failed Jobs

Each `LoadPackageInfo` has a `jobs` dict with keys:
`"started_jobs"`, `"failed_jobs"`, `"completed_jobs"`, `"new_jobs"`, `"retry_jobs"`

Failed jobs contain: job file info, error messages, retry counts.

---

## Summary: What to Hook Into for OpenLineage

### Recommended Approach

Implement the `SupportsTracking` protocol and register via `TRACKING_MODULES`.

### Data Available at Each Hook Point

| Hook | Available Data |
|------|---------------|
| `on_start_trace` | Pipeline name, destination, dataset, step type |
| `on_start_trace_step` | Same + specific step being started |
| `on_end_trace_step` | Step result (ExtractInfo/NormalizeInfo/LoadInfo or exception), timing, schemas |
| `on_end_trace` | Complete trace with all steps, full pipeline state |

### Mapping to OpenLineage Events

| dlt Event | OpenLineage Event | Key Facets |
|-----------|-------------------|------------|
| `on_start_trace` (step=run) | `RunEvent(START)` | Job name, namespace |
| `on_end_trace_step` (step=extract, success) | `RunEvent(RUNNING)` | Input datasets (from source/resource metadata), extraction metrics |
| `on_end_trace_step` (step=load, success) | `RunEvent(COMPLETE)` | Output datasets (from schema tables + destination), row counts, schema facets |
| `on_end_trace_step` (any step, failure) | `RunEvent(FAIL)` | Error message facet from `step_exception` |
| `on_end_trace` | Final `RunEvent(COMPLETE/FAIL)` | Duration, all datasets |

### Key Data Sources for OpenLineage Facets

- **Input datasets**: Source name, resource names, table hints from `ExtractInfo.metrics[load_id].hints`
- **Output datasets**: `schema.data_tables()` for table names/schemas, `LoadInfo.destination_name` + `LoadInfo.dataset_name` for namespace
- **Schema facets**: `schema.get_table_columns(table_name)` for column names, types, and hints
- **Row count facets**: `NormalizeInfo.row_counts` (Dict[table_name, count])
- **Error facets**: `PipelineStepTrace.step_exception` and `exception_traces`
- **Timing**: `PipelineStepTrace.started_at` / `finished_at`
- **Foreign keys**: `TTableSchema.references` and `schema.references`
- **Write disposition**: `TTableSchema.write_disposition` on each table
