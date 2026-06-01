# The Hitchhiker's Guide to R in Snowflake

Quarto book source. Published at [snowflake-labs.github.io/snowflakeR](https://snowflake-labs.github.io/snowflakeR/) via [Snowflake-Labs/snowflakeR](https://github.com/Snowflake-Labs/snowflakeR) (`guide/` on `main`). Develop in the monorepo under `guide/Hitchhikers_Guide_to_R_in_Snowflake/`.

## Local build

```bash
# Install Quarto: https://quarto.org/docs/get-started/
cd guide/Hitchhikers_Guide_to_R_in_Snowflake
quarto render --to html --no-execute   # fast — uses _freeze/ when present
quarto preview                          # live reload
```

To re-execute cells that call Snowflake, set `SNOWFLAKE_DEFAULT_CONNECTION_NAME` and render without `--no-execute`, then commit updated `_freeze/` directories.

## Structure

- `_quarto.yml` — book navigation and theme
- `NN_topic/index.qmd` — one chapter per folder
- `appendices/` — reference material
- `_theme/` — Snowflake-branded SCSS (shared pattern with Feature Store guide)

## Authoring

See [../GUIDE_DESIGN_AND_PLAN.md](../GUIDE_DESIGN_AND_PLAN.md) and [../_internal_development/](../_internal_development/) for migration notes and publishing workflow.
