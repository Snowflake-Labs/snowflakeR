"""
crew.spcs Worker Bridge (Strategy A)
=====================================

Python bridge for launching and managing SPCS containers as crew/mirai
workers. Each worker connects back to a controller via TCP (NNG protocol)
and processes tasks on demand.

Called from R via reticulate (snowflakeR::crew_launcher_spcs).
"""

import json
import sys
import time
import uuid
from typing import Any, Dict, List, Optional


def launch_crew_worker(
    session,
    compute_pool: str,
    image_uri: str,
    controller_url: str,
    worker_name: str = "",
    instance_family: str = "CPU_X64_S",
    warehouse: str = "",
    eai_name: str = "",
) -> Dict[str, Any]:
    """Launch a single crew worker as an EXECUTE JOB SERVICE.

    The worker runs `crew::crew_worker()` which connects back to the
    controller's TCP endpoint.

    Parameters
    ----------
    session : Snowpark Session
    compute_pool : str
    image_uri : str
        Docker image with R, crew, mirai, nanonext pre-installed.
    controller_url : str
        TCP URL of the crew controller, e.g. "tcp://svc.dns:5555".
    worker_name : str
        Unique worker identifier. Auto-generated if empty.
    instance_family : str
    warehouse : str
    eai_name : str

    Returns
    -------
    dict with worker_name, job_name, status
    """
    if not worker_name:
        worker_name = f"crew_w_{uuid.uuid4().hex[:8]}"

    job_name = f"CREW_WORKER_{worker_name.upper()}"

    res = _get_resources(instance_family)

    if not warehouse:
        try:
            wh = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()
            warehouse = wh[0]["WH"] if wh and wh[0]["WH"] else ""
        except Exception:
            warehouse = ""

    wh_env = f'\n      SNOWFLAKE_WAREHOUSE: "{warehouse}"' if warehouse else ""

    # The R command that the worker executes:
    # crew_worker() connects to the controller and processes tasks
    r_cmd = (
        f"mirai::daemon("
        f"'{controller_url}',"
        f" autoexit = mirai::daemons(NULL)"
        f")"
    )

    spec = f"""
spec:
  containers:
  - name: crew-worker
    image: {image_uri}
    command:
    - Rscript
    - -e
    - "{r_cmd}"
    env:
      WORKER_NAME: "{worker_name}"
      CONTROLLER_URL: "{controller_url}"{wh_env}
    resources:
      requests:
        cpu: {res['cpu']}
        memory: {res['memory']}
      limits:
        cpu: {res['cpu_limit']}
        memory: {res['memory_limit']}
    """.strip()

    eai_clause = ""
    if eai_name:
        eai_clause = f"\n  EXTERNAL_ACCESS_INTEGRATIONS = ({eai_name})"

    ejs_sql = f"""
        EXECUTE JOB SERVICE
        IN COMPUTE POOL {compute_pool}
        FROM SPECIFICATION $${spec}$$
        NAME = {job_name}
        ASYNC = TRUE{eai_clause};
    """

    print(f"[crew.spcs] Launching worker {worker_name} -> {controller_url}",
          file=sys.stderr)
    session.sql(ejs_sql).collect()

    return {
        "worker_name": worker_name,
        "job_name": job_name,
        "controller_url": controller_url,
        "status": "LAUNCHED",
    }


def launch_crew_workers_batch(
    session,
    compute_pool: str,
    image_uri: str,
    controller_url: str,
    n_workers: int = 4,
    instance_family: str = "CPU_X64_S",
    warehouse: str = "",
    eai_name: str = "",
) -> Dict[str, Any]:
    """Launch multiple crew workers as a single EJS with REPLICAS.

    Parameters
    ----------
    session : Snowpark Session
    compute_pool : str
    image_uri : str
    controller_url : str
    n_workers : int
    instance_family : str
    warehouse : str
    eai_name : str

    Returns
    -------
    dict with job_name, n_workers, status
    """
    job_id = uuid.uuid4().hex[:8]
    job_name = f"CREW_POOL_{job_id.upper()}"

    res = _get_resources(instance_family)

    if not warehouse:
        try:
            wh = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()
            warehouse = wh[0]["WH"] if wh and wh[0]["WH"] else ""
        except Exception:
            warehouse = ""

    wh_env = f'\n      SNOWFLAKE_WAREHOUSE: "{warehouse}"' if warehouse else ""

    r_cmd = (
        f"mirai::daemon("
        f"'{controller_url}',"
        f" autoexit = mirai::daemons(NULL)"
        f")"
    )

    spec = f"""
spec:
  containers:
  - name: crew-worker
    image: {image_uri}
    command:
    - Rscript
    - -e
    - "{r_cmd}"
    env:
      CONTROLLER_URL: "{controller_url}"{wh_env}
    resources:
      requests:
        cpu: {res['cpu']}
        memory: {res['memory']}
      limits:
        cpu: {res['cpu_limit']}
        memory: {res['memory_limit']}
    """.strip()

    eai_clause = ""
    if eai_name:
        eai_clause = f"\n  EXTERNAL_ACCESS_INTEGRATIONS = ({eai_name})"

    ejs_sql = f"""
        EXECUTE JOB SERVICE
        IN COMPUTE POOL {compute_pool}
        FROM SPECIFICATION $${spec}$$
        NAME = {job_name}
        ASYNC = TRUE
        REPLICAS = {n_workers}{eai_clause};
    """

    print(f"[crew.spcs] Launching {n_workers} workers -> {controller_url}",
          file=sys.stderr)
    session.sql(ejs_sql).collect()

    return {
        "job_name": job_name,
        "n_workers": n_workers,
        "controller_url": controller_url,
        "status": "LAUNCHED",
    }


def create_controller_service(
    session,
    compute_pool: str,
    image_uri: str,
    service_name: str = "CREW_CONTROLLER",
    listener_port: int = 5555,
    instance_family: str = "CPU_X64_S",
    r_script_stage_path: str = "",
    stage_name: str = "",
) -> Dict[str, Any]:
    """Create a long-lived SPCS service to host the crew controller.

    The controller listens on a TCP endpoint that workers connect to.
    Returns the service DNS name that workers should use.

    Parameters
    ----------
    session : Snowpark Session
    compute_pool : str
    image_uri : str
    service_name : str
    listener_port : int
    instance_family : str
    r_script_stage_path : str
        Optional R script for the controller to run (sourced at startup).
    stage_name : str
        Stage to mount (if controller needs file access).

    Returns
    -------
    dict with service_name, dns_name, listener_port, status
    """
    res = _get_resources(instance_family)

    startup_cmd = "Rscript -e 'cat(\"Controller ready\\n\"); Sys.sleep(Inf)'"
    if r_script_stage_path:
        startup_cmd = f"Rscript {r_script_stage_path}"

    volume_spec = ""
    mount_spec = ""
    if stage_name:
        mount_spec = """
    volumeMounts:
    - name: stage-vol
      mountPath: /stage"""
        volume_spec = f"""
  volumes:
  - name: stage-vol
    source: "{stage_name}"
    uid: 1000
    gid: 1000"""

    spec = f"""
spec:
  containers:
  - name: crew-controller
    image: {image_uri}
    command:
    - bash
    - -c
    - "{startup_cmd}"
    resources:
      requests:
        cpu: {res['cpu']}
        memory: {res['memory']}
      limits:
        cpu: {res['cpu_limit']}
        memory: {res['memory_limit']}{mount_spec}
  endpoints:
  - name: mirai-listener
    port: {listener_port}
    protocol: TCP
    public: false{volume_spec}
    """.strip()

    session.sql(f"""
        CREATE SERVICE IF NOT EXISTS {service_name}
        IN COMPUTE POOL {compute_pool}
        MIN_INSTANCES = 1
        MAX_INSTANCES = 1
        FROM SPECIFICATION $${spec}$$
    """).collect()

    # Get the DNS name
    try:
        dns_result = session.sql(
            f"SELECT SYSTEM$GET_SERVICE_DNS_DOMAIN('{service_name}')"
        ).collect()
        dns_domain = str(dns_result[0][0]) if dns_result else ""
        dns_name = f"{service_name.lower()}.{dns_domain}"
    except Exception:
        dns_name = f"{service_name.lower()}.svc.spcs.internal"

    return {
        "service_name": service_name,
        "dns_name": dns_name,
        "listener_port": listener_port,
        "controller_url": f"tcp://{dns_name}:{listener_port}",
        "status": "CREATED",
    }


def terminate_worker(session, job_name: str) -> bool:
    """Terminate a crew worker job."""
    try:
        session.sql(f"DROP SERVICE IF EXISTS {job_name} FORCE").collect()
        return True
    except Exception:
        return False


def get_worker_status(session, job_name: str) -> Dict[str, Any]:
    """Check status of a crew worker job."""
    try:
        result = session.sql(
            f"SELECT SYSTEM$GET_SERVICE_STATUS('{job_name}')"
        ).collect()
        if not result or not result[0][0]:
            return {"status": "NOT_FOUND"}

        instances = json.loads(str(result[0][0]))
        return {
            "status": "RUNNING" if instances else "STOPPED",
            "instances": instances,
        }
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


def _get_resources(instance_family: str) -> Dict[str, Any]:
    """Resource sizing for instance families."""
    families = {
        "CPU_X64_XS": {"cpu": 1, "memory": "4Gi", "cpu_limit": 2, "memory_limit": "6Gi"},
        "CPU_X64_S":  {"cpu": 3, "memory": "12Gi", "cpu_limit": 6, "memory_limit": "24Gi"},
        "CPU_X64_M":  {"cpu": 6, "memory": "24Gi", "cpu_limit": 12, "memory_limit": "48Gi"},
        "CPU_X64_L":  {"cpu": 12, "memory": "48Gi", "cpu_limit": 24, "memory_limit": "96Gi"},
        "CPU_X64_XL": {"cpu": 24, "memory": "96Gi", "cpu_limit": 48, "memory_limit": "192Gi"},
    }
    return families.get(instance_family.upper(), families["CPU_X64_S"])
