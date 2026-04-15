"""
Generalised R Executor Bridge (Strategy B)
==========================================

Python bridge for launching generic R executor containers on SPCS via
EXECUTE JOB SERVICE. The executor loads R scripts from a mounted Snowflake
stage, eliminating Docker image rebuilds when R code changes.

Called from R via reticulate (snowflakeR::sfr_execute_r_script).
"""

import json
import os
import sys
import tempfile
import time
import uuid
from typing import Any, Dict, List, Optional


def launch_executor(
    session,
    compute_pool: str,
    image_uri: str,
    r_script_stage_path: str,
    stage_name: str,
    config: Optional[Dict[str, Any]] = None,
    replicas: int = 1,
    entry_fn: str = "main",
    output_mode: str = "stage",
    output_format: str = "rds",
    output_path: str = "",
    instance_family: str = "CPU_X64_S",
    warehouse: str = "",
    eai_name: str = "",
    async_mode: bool = True,
    job_name: str = "",
) -> Dict[str, Any]:
    """Launch a generic R executor on SPCS.

    Parameters
    ----------
    session : Snowpark Session
    compute_pool : str
        SPCS compute pool name.
    image_uri : str
        Docker image URI containing executor.R and base R packages.
    r_script_stage_path : str
        Path to the R script *on the mounted stage* (relative to mount),
        e.g. "/stage/scripts/optimise.R".
    stage_name : str
        Fully-qualified stage name to mount, e.g. "@DB.SCHEMA.MY_STAGE".
    config : dict, optional
        Configuration to pass to the R entry function. Written as JSON
        to the stage and referenced via R_CONFIG env var.
    replicas : int
        Number of parallel workers.
    entry_fn : str
        Name of the R function to call (default: "main").
    output_mode : str
        "stage", "table", "stdout", or "none".
    output_path : str
        Stage-relative output path. Auto-generated if empty.
    instance_family : str
        SPCS instance family for resource sizing.
    warehouse : str
        Warehouse for SQL operations inside the executor.
    eai_name : str
        External Access Integration name (for internet/git access).
    async_mode : bool
        If True, launch async. If False, wait for completion.
    job_name : str
        Custom job service name. Auto-generated if empty.

    Returns
    -------
    dict with keys: job_id, job_name, replicas, status
    """
    job_id = uuid.uuid4().hex[:12]

    if not job_name:
        job_name = f"R_EXECUTOR_{job_id.upper()}"

    if not output_path:
        output_path = f"/stage/results/{job_id}/"

    # Resource sizing
    resources = _get_resources(instance_family)

    # Resolve warehouse
    if not warehouse:
        try:
            wh_rows = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()
            warehouse = wh_rows[0]["WH"] if wh_rows and wh_rows[0]["WH"] else ""
        except Exception:
            warehouse = ""

    # Upload config to stage if provided
    config_path = ""
    if config:
        config_stage_path = f"{stage_name}/executor_config/{job_id}.json"
        _upload_config(session, config, config_stage_path)
        config_path = f"/stage/executor_config/{job_id}.json"

    # Build env block
    env_vars = {
        "R_SCRIPT": r_script_stage_path,
        "R_ENTRY_FN": entry_fn,
        "OUTPUT_MODE": output_mode,
        "OUTPUT_FORMAT": output_format,
        "OUTPUT_PATH": output_path,
        "STAGE_MOUNT": "/stage",
    }
    if config_path:
        env_vars["R_CONFIG"] = config_path
    if warehouse:
        env_vars["SNOWFLAKE_WAREHOUSE"] = warehouse

    env_yaml = "\n".join(f'      {k}: "{v}"' for k, v in env_vars.items())

    # Build the EXECUTE JOB SERVICE spec
    spec = f"""
spec:
  containers:
  - name: r-executor
    image: {image_uri}
    env:
{env_yaml}
    volumeMounts:
    - name: stage-vol
      mountPath: /stage
    resources:
      requests:
        cpu: {resources['cpu']}
        memory: {resources['memory']}
      limits:
        cpu: {resources['cpu_limit']}
        memory: {resources['memory_limit']}
  volumes:
  - name: stage-vol
    source: "{stage_name}"
    uid: 1000
    gid: 1000
    """.strip()

    eai_clause = ""
    if eai_name:
        eai_clause = f"\n  EXTERNAL_ACCESS_INTEGRATIONS = ({eai_name})"

    async_clause = "ASYNC = TRUE" if async_mode else ""

    ejs_sql = f"""
        EXECUTE JOB SERVICE
        IN COMPUTE POOL {compute_pool}
        FROM SPECIFICATION $${spec}$$
        NAME = {job_name}
        {async_clause}
        REPLICAS = {replicas}
        QUERY_WAREHOUSE = {warehouse}{eai_clause};
    """

    print(f"[executor] Launching {replicas} replica(s) in {compute_pool}",
          file=sys.stderr)

    result = session.sql(ejs_sql).collect()

    return {
        "job_id": job_id,
        "job_name": job_name,
        "replicas": replicas,
        "status": "LAUNCHED",
        "output_path": output_path,
    }


def poll_executor_status(
    session,
    job_name: str,
    timeout_sec: int = 3600,
    poll_sec: int = 10,
) -> Dict[str, Any]:
    """Poll an executor job until completion.

    Returns
    -------
    dict with status, elapsed_sec, instance_statuses
    """
    import json as _json

    start = time.time()

    while True:
        elapsed = time.time() - start
        if elapsed > timeout_sec:
            return {"status": "TIMEOUT", "elapsed_sec": round(elapsed, 1)}

        try:
            result = session.sql(
                f"SELECT SYSTEM$GET_SERVICE_STATUS('{job_name}')"
            ).collect()

            if not result or not result[0][0]:
                return {
                    "status": "COMPLETED",
                    "elapsed_sec": round(time.time() - start, 1),
                }

            status_str = str(result[0][0])
            if status_str in ("[]", ""):
                return {
                    "status": "COMPLETED",
                    "elapsed_sec": round(time.time() - start, 1),
                }

            instances = _json.loads(status_str)

            all_done = all(
                inst.get("status") in ("DONE", "FAILED")
                for inst in instances
            )
            any_failed = any(
                inst.get("status") == "FAILED"
                for inst in instances
            )

            if all_done:
                return {
                    "status": "FAILED" if any_failed else "COMPLETED",
                    "elapsed_sec": round(time.time() - start, 1),
                    "instances": instances,
                }

        except Exception as e:
            if "does not exist" in str(e).lower():
                return {
                    "status": "COMPLETED",
                    "elapsed_sec": round(time.time() - start, 1),
                }

        time.sleep(poll_sec)


def collect_results(
    session,
    stage_name: str,
    output_path: str,
    local_dir: str = "/tmp/executor_results",
) -> List[str]:
    """Download executor results from stage to local directory.

    Returns list of local file paths.
    """
    os.makedirs(local_dir, exist_ok=True)

    stage_path = f"{stage_name}/{output_path.lstrip('/')}"
    try:
        session.sql(
            f"GET '{stage_path}' 'file://{local_dir}/' PARALLEL=4"
        ).collect()
    except Exception as e:
        print(f"[executor] Warning: GET failed: {e}", file=sys.stderr)

    files = []
    if os.path.isdir(local_dir):
        for f in os.listdir(local_dir):
            files.append(os.path.join(local_dir, f))

    return files


def upload_r_script(
    session,
    local_script_path: str,
    stage_name: str,
    stage_dir: str = "scripts",
) -> str:
    """Upload an R script to a Snowflake stage.

    Returns the stage path for use in R_SCRIPT env var.
    """
    basename = os.path.basename(local_script_path)
    stage_path = f"{stage_name}/{stage_dir}/"

    session.sql(
        f"PUT 'file://{local_script_path}' '{stage_path}' "
        f"AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
    ).collect()

    return f"/stage/{stage_dir}/{basename}"


def _upload_config(session, config: dict, stage_path: str) -> None:
    """Write config dict as JSON to a stage path."""
    tmp = os.path.join(tempfile.gettempdir(), f"config_{uuid.uuid4().hex[:8]}.json")
    with open(tmp, "w") as f:
        json.dump(config, f, indent=2)

    stage_dir = os.path.dirname(stage_path)
    session.sql(
        f"PUT 'file://{tmp}' '{stage_dir}/' "
        f"AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
    ).collect()
    os.unlink(tmp)


def _get_resources(instance_family: str) -> Dict[str, Any]:
    """Return resource requests/limits for the given instance family."""
    families = {
        "CPU_X64_XS": {"cpu": 1, "memory": "4Gi", "cpu_limit": 2, "memory_limit": "6Gi"},
        "CPU_X64_S":  {"cpu": 3, "memory": "12Gi", "cpu_limit": 6, "memory_limit": "24Gi"},
        "CPU_X64_M":  {"cpu": 6, "memory": "24Gi", "cpu_limit": 12, "memory_limit": "48Gi"},
        "CPU_X64_L":  {"cpu": 12, "memory": "48Gi", "cpu_limit": 24, "memory_limit": "96Gi"},
        "CPU_X64_XL": {"cpu": 24, "memory": "96Gi", "cpu_limit": 48, "memory_limit": "192Gi"},
    }
    return families.get(
        instance_family.upper(),
        families["CPU_X64_S"]
    )
