# snowflakeR

R interface to the Snowflake ML platform -- Model Registry, Feature Store, Datasets, and SPCS model serving. Works in **local R environments** (RStudio, VS Code, terminal) and **Snowflake Workspace Notebooks**.

> **Companion package:** For standard DBI-compliant database access (`dbGetQuery`, `dbWriteTable`, `dbplyr`, RStudio Connections Pane, etc.), see [**RSnowflake**](https://github.com/Snowflake-Labs/RSnowflake). `snowflakeR` focuses on ML platform features; `RSnowflake` provides the database connectivity layer.

## Overview

**snowflakeR** provides idiomatic R functions for the full Snowflake ML lifecycle, whether you're working locally or directly inside Snowflake:

| Module | What it does |
|---|---|
| **Connect** | One-line connection via `connections.toml`, keypair, or Workspace auto-detect |
| **Query** | Run SQL via `sfr_query()` / `sfr_execute()`, read/write tables |
| **Model Registry** | Log R models (lm, glm, randomForest, xgboost, ...), deploy to SPCS, run inference |
| **Feature Store** | Create entities & feature views, generate training data, retrieve features at inference |
| **Datasets** | Versioned, immutable snapshots of query results for reproducible ML |
| **Admin** | Manage compute pools, image repos, and external access integrations |
| **REST Inference** | Pure-R prediction against SPCS service endpoints via `sfr_predict_rest()` |
| **Workspace Notebooks** | First-class support for Snowflake Workspace Notebooks with zero-config auth and `%%R` magic cells |

Under the hood, `snowflakeR` uses [`reticulate`](https://rstudio.github.io/reticulate/) to bridge to the [`snowflake-ml-python`](https://docs.snowflake.com/en/developer-guide/snowpark-ml/index) SDK while exposing a native R API with `snake_case` naming, S3 classes, and `cli` messaging. The same code runs identically in local R sessions and Snowflake Workspace Notebooks.

### DBI / dbplyr

For full DBI compliance, `dbplyr` integration, and the RStudio Connections Pane, use the companion **RSnowflake** package. You can obtain an `RSnowflake` connection from an existing `sfr_connection`:

```r
dbi_con <- sfr_dbi_connection(conn)  # lazy, cached on first call
DBI::dbGetQuery(dbi_con, "SELECT 1")

library(dplyr)
tbl(dbi_con, "MY_TABLE") |> filter(score > 90) |> collect()
```

Or connect directly with RSnowflake:

```r
library(DBI)
library(RSnowflake)
con <- dbConnect(Snowflake(), name = "my_profile")
```

## Installation

```r
# Install from GitHub
# install.packages("pak")
pak::pak("Snowflake-Labs/snowflakeR")
```

### Python dependencies

snowflakeR requires Python >= 3.9 with the Snowflake ML packages:

```r
# Let snowflakeR install them into a dedicated virtualenv
snowflakeR::sfr_install_python_deps()
```

Or install manually:

```bash
pip install snowflake-ml-python snowflake-snowpark-python
```

## Quick start

```r
library(snowflakeR)

# Connect (reads ~/.snowflake/connections.toml by default)
conn <- sfr_connect()

# Run a query
df <- sfr_query(conn, "SELECT * FROM my_table LIMIT 10")

# Log an R model to the Model Registry
model <- lm(mpg ~ wt + hp, data = mtcars)
reg   <- sfr_model_registry(conn)
sfr_log_model(reg, model, model_name = "MPG_MODEL", version = "V1")

# Create a Feature Store entity and feature view
fs <- sfr_feature_store(conn)
sfr_create_entity(fs, name = "CUSTOMER", join_keys = "CUSTOMER_ID")
fv <- sfr_create_feature_view(
  fs,
  name       = "CUSTOMER_FEATURES",
  entity     = "CUSTOMER",
  source_sql = "SELECT customer_id, avg_spend, tenure FROM feature_table"
)
sfr_register_feature_view(fs, fv, version = "V1")
```

## Snowflake Workspace Notebooks

snowflakeR is designed to work seamlessly inside [Snowflake Workspace Notebooks](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks). It auto-detects the Workspace environment, connects via the active session token (no credentials needed), and supports the `%%R` magic cell pattern for mixing Python and R:

```r
# In a Workspace Notebook %%R cell -- zero-config connection
library(snowflakeR)
conn <- sfr_connect()   # auto-detects Workspace session

# All snowflakeR functions work identically
df <- sfr_query(conn, "SELECT * FROM my_table LIMIT 10")
reg <- sfr_model_registry(conn)
fs  <- sfr_feature_store(conn)
```

The package also provides `rprint()`, `rview()`, `rglimpse()`, and `rcat()` helpers for rich output rendering in Workspace cells. See `vignette("workspace-notebooks")` for setup instructions and tips for the dual local/Workspace workflow.

> **Note:** Workspace Notebooks do [not auto-set database or schema](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks-in-workspaces/notebooks-in-workspaces-edit-run#set-the-execution-context).
> Use `setup_notebook()` from `sfnb_setup.py` to set session context
> automatically (from the YAML config or session defaults), and
> `sfr_fqn()` to build fully qualified table names
> (`DATABASE.SCHEMA.TABLE`). See the example notebooks below.

## Example Notebooks

`snowflakeR` ships with a **self-contained `notebooks/` directory** that has
everything you need to get started, including the bootstrap script and
per-notebook config files:

```r
# Find the notebooks directory in the installed package
system.file("notebooks", package = "snowflakeR")

# Or copy the entire folder to your working directory
nb_dir <- system.file("notebooks", package = "snowflakeR")
file.copy(list.files(nb_dir, full.names = TRUE), ".", recursive = TRUE)
```

For **Workspace Notebooks**, upload the folder contents to your Workspace and
open `workspace_quickstart.ipynb`. For **local** environments, open
`local_quickstart.ipynb`.

| File | Purpose |
|---|---|
| `workspace_quickstart.ipynb` | Quickstart for Snowflake Workspace Notebooks |
| `local_quickstart.ipynb` | Quickstart for local R environments |
| `workspace_model_registry.ipynb` | Model Registry: log, deploy, serve R models (Workspace) |
| `local_model_registry.ipynb` | Model Registry for local environments |
| `workspace_feature_store.ipynb` | Feature Store: entities, views, training data (Workspace) |
| `local_feature_store.ipynb` | Feature Store for local environments |
| `sfnb_setup.py` | All-in-one bootstrap: EAI, R runtime, packages, context (Workspace) |
| `snowflaker_config.yaml` | Per-notebook config: context, EAI, packages |

For Workspace Notebooks, the first cell runs `setup_notebook()` which
handles EAI validation, R installation, package installation, and session
context automatically. No separate config copy step is needed -- edit the
`_config.yaml` directly or rely on session defaults.

## Vignettes

| Vignette | Topic |
|---|---|
| `vignette("setup")` | **Prerequisites, Python setup, auth, and environment-specific config** (RStudio, VS Code, Jupyter, Workspace, Docker/CI) |
| `vignette("getting-started")` | Connecting, queries, table ops, DBI/dbplyr |
| `vignette("model-registry")` | Training, logging, deploying, and serving R models |
| `vignette("feature-store")` | Entities, feature views, training data, inference |
| `vignette("workspace-notebooks")` | Full guide for Snowflake Workspace Notebooks |

## Requirements

- R >= 4.1.0
- Python >= 3.9
- `snowflake-ml-python` >= 1.5.0
- `snowflake-snowpark-python`

Optional:

- [`RSnowflake`](https://github.com/Snowflake-Labs/RSnowflake) -- DBI-compliant database access, `dbplyr`, RStudio Connections Pane
- [`snowflakeauth`](https://github.com/Snowflake-Labs/snowflakeauth) -- `connections.toml` credential management

## License

Apache License 2.0
