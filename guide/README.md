# The Hitchhiker's Guide to R in Snowflake

Quarto book source for the end-to-end implementation guide (Workspace, RSnowflake, snowflakeR).

| | |
|---|---|
| **Published site** | [snowflake-labs.github.io/snowflakeR](https://snowflake-labs.github.io/snowflakeR/) |
| **Edit on GitHub** | [guide/Hitchhikers_Guide_to_R_in_Snowflake](https://github.com/Snowflake-Labs/snowflakeR/tree/main/guide/Hitchhikers_Guide_to_R_in_Snowflake) |
| **CI** | [publish-guide.yml](../.github/workflows/publish-guide.yml) |

## Local render

From the monorepo, source lives at `guide/Hitchhikers_Guide_to_R_in_Snowflake/` (synced here on public push).

```bash
cd guide/Hitchhikers_Guide_to_R_in_Snowflake
quarto render --to html --no-execute
quarto preview
```

## Monorepo development

Edit in `snowflake_model_reg_rpy2/guide/`, then run `bash sync_snowflakeR_to_public.sh` from the monorepo root.
