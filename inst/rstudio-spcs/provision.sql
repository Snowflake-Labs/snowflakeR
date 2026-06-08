-- One-time provisioning for RStudio SPCS service.
-- Adjust database, schema, role, and pool names before running.
--   snow sql -c my_connection -f provision.sql

USE ROLE MY_DEPLOY_ROLE;
USE WAREHOUSE MY_WH;
USE DATABASE MY_DB;
USE SCHEMA MY_SCHEMA;

CREATE STAGE IF NOT EXISTS VOLUMES
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = (ENABLE = TRUE);

-- Volume subpaths used by service-spec.template.yaml:
-- @MY_DB.MY_SCHEMA.VOLUMES/rstudio_workspace  -> /home/rstudio/workspace
-- @MY_DB.MY_SCHEMA.VOLUMES/rstudio_r_libs      -> /home/rstudio/r_packages

SHOW IMAGE REPOSITORIES;

ALTER COMPUTE POOL MY_COMPUTE_POOL RESUME;
