-- Egress for SPCS Image Builder (Docker Hub, CRAN/PPM, PyPI, Anaconda).
-- Run as ACCOUNTADMIN, then grant USAGE to your deploy role.

USE ROLE ACCOUNTADMIN;

CREATE NETWORK RULE IF NOT EXISTS MY_DB.MY_SCHEMA.IMAGE_BUILDER_EGRESS
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'registry-1.docker.io:443',
    'auth.docker.io:443',
    'production.cloudfront.docker.com:443',
    'production.cloudflare.docker.com:443',
    'archive.ubuntu.com:443',
    'security.ubuntu.com:443',
    'cloud.r-project.org:443',
    'cran.r-project.org:443',
    'packagemanager.posit.co:443',
    'rspm-sync.rstudio.com:443',
    'cdn.posit.co:443',
    'pypi.org:443',
    'files.pythonhosted.org:443',
    'bootstrap.pypa.io:443',
    'repo.anaconda.com:443',
    'conda.anaconda.org:443',
    'github.com:443',
    'api.github.com:443',
    'codeload.github.com:443'
  );

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS MY_IMAGE_BUILDER_EAI
  ALLOWED_NETWORK_RULES = (MY_DB.MY_SCHEMA.IMAGE_BUILDER_EGRESS)
  ENABLED = TRUE;

GRANT USAGE ON NETWORK RULE MY_DB.MY_SCHEMA.IMAGE_BUILDER_EGRESS TO ROLE MY_DEPLOY_ROLE;
GRANT USAGE ON INTEGRATION MY_IMAGE_BUILDER_EAI TO ROLE MY_DEPLOY_ROLE;
