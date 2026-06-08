# RStudio on Snowflake (SPCS)

Welcome. This container ships **RStudio Server** with **RSnowflake** and **snowflakeR**
for in-account R development via SPCS OAuth.

## First-time smoke tests

After Snowflake ingress SSO, log in to RStudio as user `rstudio` with the password
set in your service spec (`PASSWORD` env var).

**R Console**

```r
source("~/smoke_test.R")
```

**Terminal**

```bash
python3 ~/smoke_test.py
```

## Connect from R

```r
library(DBI)
library(RSnowflake)

con <- dbConnect(Snowflake())
dbGetQuery(con, "SELECT CURRENT_USER(), CURRENT_WAREHOUSE()")
```

```r
source("~/spcs_helpers.R")
ml <- sfr_connect_spcs()
```

## Environment

Warehouse, database, schema, and role are set via container env vars in
`service-spec.template.yaml` (not `USE WAREHOUSE` — RSnowflake uses the SQL REST API).

Python for snowflakeR: Conda env `snowflake_ml` at `/opt/conda`.
