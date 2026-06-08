# RStudio Server on SPCS

Deploy **RStudio Server** as a custom **Snowpark Container Services (SPCS)**
service with **RSnowflake** and **snowflakeR** pre-installed. R users get a
full native R IDE inside the Snowflake security perimeter.

> **Community reference implementation** — not an official Snowflake product.
> You operate the image, compute pool, ingress, and scaling.

## When to use this

| Choose RStudio on SPCS when… | Choose something else when… |
|------------------------------|----------------------------|
| You need RStudio UX (Connections pane, debugger, Quarto) | Notebook-first → [Workspace Notebooks](https://github.com/Snowflake-Labs/snowflake-notebook-multilang) |
| You want full control over the container image | Managed IDE → [Posit Workbench Native App](https://posit.co/products/enterprise/posit-workbench/) |
| Posit licensing or Marketplace is not an option yet | VS Code / Cursor → Snowflake Remote Dev |

**Documentation**

- Hitchhiker's Guide: [RStudio Server on SPCS](https://snowflake-labs.github.io/snowflakeR/04b_rstudio_spcs/index.html) (walkthrough)
- Vignettes: `vignette("rstudio-spcs", package = "snowflakeR")`, `vignette("spcs-custom-services", package = "RSnowflake")`

## Kit location

After installing snowflakeR:

```r
system.file("rstudio-spcs", package = "snowflakeR")
```

Or clone [Snowflake-Labs/snowflakeR](https://github.com/Snowflake-Labs/snowflakeR) and `cd inst/rstudio-spcs`.

## Quick start

### 1. Prerequisites

- SPCS **compute pool** and **image repository**
- Role with `CREATE SERVICE`, image repo **OWNERSHIP** (for Image Builder), pool USAGE
- Snowflake CLI (`snow`) with a configured connection
- Optional: **SPCS Image Builder** preview (CLI ≥ 3.16, `enable_spcs_build_image = true`)

### 2. Configure

```bash
cd inst/rstudio-spcs   # or path from system.file() above
cp config.example.env config.env
# Edit config.env — account, registry, pool, passwords, roles
```

Provision Snowflake objects from the same `config.env` (no manual SQL editing):

```bash
./provision.sh
./provision.sh --eai    # once, with SNOW_EAI_ROLE (default ACCOUNTADMIN)
```

### 3. Build image

**Preferred — in-account (Image Builder):**

```bash
source config.env
./build_snowflake.sh
```

**Fallback — local Docker (always `linux/amd64`):**

```bash
source config.env
./build_local.sh
PUSH=1 ./build_local.sh
```

`prepare_build_ctx.sh` builds a **snowflakeR** tarball from the package source.
**RSnowflake** is included when the monorepo sibling exists; otherwise
`install_packages.R` installs from GitHub during the image build (requires EAI).

### 4. Deploy service

```bash
source config.env
./deploy_service.sh
```

Fetch the ingress URL (it **changes** after each `DROP SERVICE` + `CREATE SERVICE`):

```sql
SHOW ENDPOINTS IN SERVICE MY_DB.MY_SCHEMA.RSTUDIO_SVC;
```

### 5. Smoke test

Log in via ingress SSO, then RStudio (`rstudio` / password from `config.env`):

```r
source("~/smoke_test.R")
```

## Files

| File | Purpose |
|------|---------|
| `config.example.env` | Connection, registry, pool, service names |
| `provision.sh` | Render + run `provision.sql.template` / EAI template |
| `service-spec.template.yaml` | SPCS service spec (rendered by `deploy_service.sh`) |
| `Dockerfile.imagebuilder` | Image recipe (Miniconda + R packages) |
| `prepare_build_ctx.sh` | Flat build context for Image Builder |
| `build_snowflake.sh` / `build_local.sh` | In-account vs local build |
| `deploy_service.sh` | `CREATE SERVICE` |
| `spcs_helpers.R` | `sfr_connect_spcs()` for custom SPCS (not Workspace) |
| `smoke_test.R` / `smoke_test.py` | End-to-end validation |

## Key technical notes

- **RSnowflake:** `dbConnect(Snowflake())` uses `SNOWFLAKE_HOST` + `/snowflake/session/token`. Set `SNOWFLAKE_WAREHOUSE` in the service spec — `USE WAREHOUSE` is not supported on the SQL REST API.
- **snowflakeR:** `sfr_connect()` auto-detects Workspace only. Use `sfr_connect_spcs()` in this container.
- **reticulate:** Image uses Miniconda `snowflake_ml` env; `use_python(full_path)` not `use_condaenv()`.
- **Stage I/O:** RSnowflake REST API has no `GET`/`PUT` — use stage volume mounts for file transfer.

See [BUILD.md](BUILD.md) for Image Builder vs local Docker comparison and troubleshooting.
