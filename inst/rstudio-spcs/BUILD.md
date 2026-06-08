# Building the RStudio SPCS Image

## Two build paths

| | **SPCS Image Builder** | **Local Docker + push** |
|---|------------------------|-------------------------|
| **Command** | `snow spcs service build-image` | `docker build` + `docker push` |
| **CLI** | ≥ 3.16 + preview flag | Any Docker install |
| **What you upload** | Flat `build-ctx/` (~few MB) | Full image layers (~2–4 GB) |
| **Where build runs** | SPCS compute pool | Your laptop |
| **Platform** | amd64 enforced | `--platform linux/amd64` required on Apple Silicon |

## Image Builder setup

1. Snowflake CLI ≥ 3.16 with feature flag:

   ```toml
   # ~/.snowflake/config.toml
   [cli.feature_flags]
   enable_spcs_build_image = true
   ```

2. **OWNERSHIP** on the image repository (READ/WRITE alone is insufficient).

3. **EAI** for build egress — see `provision_image_builder_eai.sql`.

4. Build:

   ```bash
   source config.env
   ./build_snowflake.sh
   ```

### Image Builder limitations (preview)

- Flat build context only (no nested directories).
- `FROM rocker/rstudio` pulls from Docker Hub via EAI.
- Very large images may hit ephemeral disk limits; this RStudio image is typically fine.

## Local build

```bash
source config.env
./build_local.sh
PUSH=1 ./build_local.sh
```

Always pass `--platform linux/amd64` for SPCS (enforced in `build_local.sh`).

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `build-image` command missing | Upgrade Snow CLI ≥ 3.16; enable feature flag |
| `exec format error` in service | Rebuild with `linux/amd64` |
| Build can't pull `rocker/rstudio` | Add Docker Hub hosts to build EAI |
| `dbConnect()` fails | Verify `SNOWFLAKE_HOST` and token file in container |
| Ingress URL missing | `SHOW ENDPOINTS IN SERVICE ...` after deploy |
| Stale image on pool | Suspend/resume compute pool after push |

## References

- [SPCS image registry](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository)
- [Connecting from inside a container](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/additional-considerations-services-jobs#connecting-to-snowflake-from-inside-a-container)
