# Embarrassingly parallel R on SPCS (general demo)

**Notebooks:** `workspace_parallel_spcs_setup.ipynb` (once) → `workspace_parallel_spcs_demo.ipynb` (driver) + optionally `workspace_parallel_spcs_monitor.ipynb` (SQL while jobs run).  
**Streamlit:** `streamlit_parallel_demo_monitor.py` (SiS; polls the same tables).  
**Single source of truth:** `snowflaker_parallel_spcs_config.yaml` → top-level key **`parallel_lab`** (database, schemas, warehouse, `compute_pool`, `image_uri`, stage name, queue table). Loaded by **`parallel_lab_config.py`** into environment variables (`PARALLEL_LAB_*`) for R cells and into Python dicts for the monitor notebook and Streamlit.

**Driver scale (optional keys under `parallel_lab`):** `demo_forecast_n_skus`, `demo_tasks_chunks_per_job`, `demo_queue_n_workers`, `demo_queue_chunks_per_job` — defaults mirror `internal/doSnowflake/tests/test_dosnowflake_tasks_benchmark.py` (2000 SKUs, 10-way parallelism).

**Internal benchmarks** (`internal/doSnowflake/tests/*.py`) still use **hard-coded** Python constants — they are **not** wired to this YAML. The checked-in `snowflaker_parallel_spcs_config.yaml` now defaults to a generic clean-room setup (`SFLAB_EP_*`) with `create_synthetic_series_table: true` so the demo can run without account-specific source tables.

## Workload framing

The runnable example uses **per–time-series forecasting** (`forecast` package) as a **toy parallel unit**. The same orchestration applies to any **embarrassingly parallel** workload where units are independent or lightly coupled:

- Hyper-parameter search grids  
- Rolling **back-tests**  
- **Monte Carlo** / bootstrap replicates  
- SKU-level optimisers, simulations, or scoring  

Call this out in the demo notebook introduction.

## Monitoring while the driver cell is blocked

`foreach %dopar%` in **queue** or **tasks** mode runs until Snowflake work completes; the **driver notebook kernel blocks**, so you cannot run other cells in that same notebook session concurrently.

**Recommended pattern (two surfaces):**

1. **Streamlit in Snowflake** — open the monitor app in a **second browser tab** before starting the long-running driver cell. Streamlit auto-refreshes and runs SQL against `CONFIG` progress / queue / manifest tables. This is the primary “live ops” experience.

2. **Second notebook** — `workspace_parallel_spcs_monitor.ipynb` contains only **setup + SQL/R query cells** (no long-running `%dopar%`). Keep it open in another Workspace tab; run “Refresh” cells while the driver notebook is busy. Same SQL as Streamlit, for users who prefer Worksheets-style monitoring.

**Not recommended:** trying to overlap execution in one kernel without a separate process (adds complexity and is fragile in Workspace).

## Batch inference: SKU bundles in the partition UDTF vs stage-write

When serving through the **Snowflake Model Registry** batch path (e.g. `run_batch` / partitioned table function over a scoring table):

| Approach | Role |
|----------|------|
| **Many tiny partitions (one SKU per partition)** | High scheduler / Ray / partition overhead; poor at small batch sizes (see internal shuffle/partition notes for partition-heavy APIs). |
| **Fewer partitions, each processing a bundle of SKUs** | **Amortises** model load and invocation overhead; each partition returns **many rows** (all forecasts in that bundle). Usually **faster end-to-end** for Registry-driven batch inference than per-SKU partitions. |
| **Workers write Parquet/CSV to stage, then COPY** | Strong for **bulk materialisation** and avoiding inference API limits; bypasses the Registry **serving** path for the write phase — best when you are **not** trying to demonstrate Registry batch scoring. |

**Choice for this demo (single path):** use **bundled partitions** in the Model Registry batch inference API (partition column = **bundle id**, input table carries all rows for SKUs in that bundle, UDTF handler returns a **wide result set** of forecasts for the whole bundle). Do **not** also implement a parallel “stage-only” inference pipeline in the same demo — that duplicates the story and optimises for a different integration point.

If later benchmarks show stage-write wins for a **specific** account workload, that becomes an advanced variant, not the default lab narrative.
