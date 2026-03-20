# R on Snowflake: Three Packages, One Platform

## The Problem

Data scientists working in R have historically not had access to Snowflake's ML platform. They have been able to work with Snowflake data via database connections and dplyr support, but there was no integration to the ML features.

Feature Store, Model Registry, and Snowpark ML provided Python-first API's.
Teams using R for statistical modeling, time-series forecasting, or marketing analytics had to maintain separate infrastructure or rewrite everything in Python.

## The Solution: Three Open-Source Packages

### 1. `snowflake-notebook-multilang`
**What:** Setup toolkit for Snowflake Workspace Notebooks.
Bootstraps R (and Julia, Scala) inside the notebook environment via `micromamba`,
installs packages, and configures the `%%R` cell magic.

**Repo:** [github.com/Snowflake-Labs/snowflake-notebook-multilang](https://github.com/Snowflake-Labs/snowflake-notebook-multilang)

### 2. `RSnowflake`
**What:** DBI-compliant database driver for Snowflake from R.
Supports REST API, ADBC (Arrow-native), and SPCS OAuth authentication.
Enables `dbplyr` for dplyr-to-SQL translation -- write R, execute in Snowflake.

**Repo:** [github.com/Snowflake-Labs/RSnowflake](https://github.com/Snowflake-Labs/RSnowflake)

### 3. `snowflakeR`
**What:** R interface to Snowflake's ML platform -- Feature Store, Model Registry,
Datasets, and SPCS deployment. Train models in R, register them in Snowflake,
and serve them in containers. Full ML Lineage support.

**Repo:** [github.com/Snowflake-Labs/snowflakeR](https://github.com/Snowflake-Labs/snowflakeR)

## Architecture

```
┌──────────────────────────────────────────────────────┐
│            Snowflake Workspace Notebook              │
│                                                      │
│   Python cells          R cells (%%R magic)          │
│       │                       │                      │
│       │              ┌────────┴────────┐             │
│       │              │   RSnowflake    │  DBI/dbplyr │
│       │              │   snowflakeR    │  ML platform│
│       │              └────────┬────────┘             │
│       │                       │                      │
│       │              Python bridge (rpy2)            │
│       └───────────┬───────────┘                      │
│                   │                                  │
│         Snowpark Session (SPCS OAuth)                │
└───────────────────┼──────────────────────────────────┘
                    │
    ┌───────────────┼───────────────┐
    │               │               │
Feature Store   Model Registry   Warehouse
 (Entities,     (Versioning,     (SQL compute,
  Feature Views,  Metrics,        Dynamic Tables,
  Datasets)       SPCS Deploy)    dbplyr queries)
```

## Key Design Principles

- **Zero-config authentication** -- In Workspace Notebooks, all three packages use
  the built-in SPCS OAuth token automatically. No credentials, no PATs, no config files.
  Just `sfr_connect()` and you're in.
- **Automatic network setup** -- R package installation requires outbound internet access
  (CRAN, GitHub, conda-forge). The setup process detects or creates the required
  External Access Integration (EAI) and network rules automatically. If the user lacks
  `CREATE INTEGRATION` privileges, a SQL script is provided for an admin to run.
- **No Python required from the R user** -- `snowflakeR` handles the Python bridge internally
- **Snowflake-native objects** -- Feature Views, Datasets, and Models created in R are
  visible and usable from Python (and vice versa)
- **Train in R, serve in Snowflake** -- Models are wrapped in a Python `CustomModel`
  with `rpy2`, deployed to SPCS containers with R pre-installed
- **ML Lineage** -- Full traceability: Source Table → Feature View → Dataset → Model

## Today's Demo

**Marketing Analytics: CausalImpact & Robyn on Snowflake**

An end-to-end workflow using real marketing data:

1. **Setup** -- `snowflake-notebook-multilang` bootstraps R + packages (~2 min)
2. **Connect** -- `RSnowflake` provides DBI connectivity, `dbplyr` for lazy SQL
3. **Feature Store** -- `snowflakeR` creates Feature Views from dplyr pipelines
4. **CausalImpact** -- Bayesian causal inference to measure campaign lift
5. **Model Registry** -- Register the bsts model with full ML Lineage
6. **Robyn** -- Marketing Mix Modeling with Meta's open-source MMM framework
7. **SQL Inference** -- Pre-computed response curves served as pure Snowflake SQL
