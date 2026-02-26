# snowflakeR Package Notebooks

Interactive Jupyter notebooks demonstrating the `snowflakeR` R package for
Snowflake ML workflows. These work in both **local environments** (RStudio,
Posit Workbench, JupyterLab) and **Snowflake Workspace Notebooks**.

## Notebooks

| Notebook | Description |
|----------|-------------|
| `quickstart.ipynb` | Connect, query, DBI/dbplyr, visualization |
| `model_registry_demo.ipynb` | Train R models, register, predict, manage versions |
| `feature_store_demo.ipynb` | Entities, Feature Views, training data, end-to-end ML |

## Quick Start

### Local (RStudio / JupyterLab)

```r
install.packages("snowflakeR")   # or pak::pak("Snowflake-Labs/snowflakeR")
library(snowflakeR)
sfr_install_python_deps()        # one-time Python env setup
conn <- sfr_connect()            # reads ~/.snowflake/connections.toml
```

### Snowflake Workspace Notebook

```python
# Python cell -- install R + snowflakeR
from r_helpers import setup_r_environment
setup_r_environment()
```

```r
%%R
library(snowflakeR)
conn <- sfr_connect()   # auto-detects Workspace session
```

## Relationship to Original Notebooks

The parent directory (`r_notebook/`) contains the original pre-package
notebooks that use manual `rpy2` setup and `source("snowflake_registry.R")`.
Those are preserved for historical reference. These notebooks demonstrate
the same workflows using the `snowflakeR` package API.
