"""
doSnowflake Tasks Bridge
========================
Python bridge for creating, running, and monitoring Snowflake Task graphs
that orchestrate SPCS job services for parallel R foreach execution.

Called from R via reticulate.
"""

from typing import Any, Dict, Optional


# "packed" profile: see sfr_queue_bridge.py for rationale and node specs.
# Limits are set to the per-container share (node_cpu / containers_per_node),
# NOT full node capacity.  Setting limits too high causes detectCores() to
# return the limit value, which makes mclapply fork far more processes than
# physical cores available — destroying throughput via context switching.
INSTANCE_FAMILY_RESOURCES = {
    "CPU_X64_XS": {"cpu": 1, "memory": "4Gi",  "cpu_lim": 1,  "mem_lim": "6Gi"},
    "CPU_X64_S":  {"cpu": 2, "memory": "6Gi",  "cpu_lim": 3,  "mem_lim": "13Gi"},
    "CPU_X64_M":  {"cpu": 3, "memory": "12Gi", "cpu_lim": 6,  "mem_lim": "14Gi"},
    "CPU_X64_SL": {"cpu": 6, "memory": "24Gi", "cpu_lim": 7,  "mem_lim": "27Gi"},
    "CPU_X64_L":  {"cpu": 6, "memory": "24Gi", "cpu_lim": 7,  "mem_lim": "29Gi"},
}


def create_and_run_dag(
    session,
    dag_name: str,
    job_id: str,
    n_chunks: int,
    stage_path: str,
    compute_pool: str,
    image_uri: str,
    warehouse: Optional[str] = None,
    instance_family: str = "CPU_X64_S",
) -> Dict[str, Any]:
    """Build a Task DAG with one SPCS job per chunk and execute it.

    Each DAGTask runs EXECUTE JOB SERVICE with environment variables
    pointing the worker container at its chunk on the stage.

    Args:
        session: Snowpark session.
        dag_name: Unique name for this task graph.
        job_id: UUID identifying this foreach job.
        n_chunks: Number of parallel chunks (one SPCS job each).
        stage_path: Full stage path (e.g. @DB.SCHEMA.STAGE/job_uuid).
        compute_pool: Name of the SPCS compute pool.
        image_uri: Full image URI (e.g. /db/schema/repo/image:tag).
        warehouse: Optional warehouse for non-serverless tasks.
        instance_family: SPCS instance family for container resource sizing.

    Returns:
        Dict with dag_name and status.
    """
    from snowflake.core import Root
    from snowflake.core.task.dagv1 import DAG, DAGOperation, DAGTask

    root = Root(session)

    dag_kwargs = {"name": dag_name}
    if warehouse:
        dag_kwargs["warehouse"] = warehouse

    with DAG(**dag_kwargs) as dag:
        for i in range(n_chunks):
            chunk_id = f"{i + 1:03d}"
            spec = _build_job_spec(
                job_id=job_id,
                chunk_id=chunk_id,
                stage_path=stage_path,
                image_uri=image_uri,
                instance_family=instance_family,
            )
            DAGTask(
                f"chunk_{chunk_id}",
                definition=(
                    f"EXECUTE JOB SERVICE "
                    f"IN COMPUTE POOL {compute_pool} "
                    f"FROM SPECIFICATION $${spec}$$"
                ),
            )

    schema = root.databases[session.get_current_database()].schemas[
        session.get_current_schema()
    ]
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag, mode="orreplace")
    dag_op.run(dag)

    return {"dag_name": dag_name, "status": "STARTED", "n_chunks": n_chunks}


def get_dag_status(session, dag_name: str) -> Dict[str, Any]:
    """Check the run status of a task graph.

    Queries TASK_HISTORY for the root task to determine overall DAG status.

    Args:
        session: Snowpark session.
        dag_name: Name of the root task / DAG.

    Returns:
        Dict with state, error (if any), and timing info.
    """
    result = (
        session.sql(
            f"""
        SELECT STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            TASK_NAME => '{dag_name}',
            RESULT_LIMIT => 1
        ))
        ORDER BY SCHEDULED_TIME DESC
        LIMIT 1
    """
        )
        .collect()
    )

    if len(result) == 0:
        return {"state": "UNKNOWN", "error": None}

    row = result[0]
    return {
        "state": str(row["STATE"]),
        "error": str(row["ERROR_MESSAGE"]) if row["ERROR_MESSAGE"] else None,
        "scheduled_time": (
            str(row["SCHEDULED_TIME"]) if row["SCHEDULED_TIME"] else None
        ),
        "completed_time": (
            str(row["COMPLETED_TIME"]) if row["COMPLETED_TIME"] else None
        ),
    }


def get_dag_child_status(session, dag_name: str) -> Dict[str, Any]:
    """Get status of all child tasks in the DAG.

    Useful for progress reporting and identifying which chunks failed.

    Args:
        session: Snowpark session.
        dag_name: Name of the root task / DAG.

    Returns:
        Dict with total, completed, failed, running counts and per-chunk details.
    """
    try:
        result = (
            session.sql(
                f"""
            SELECT NAME, STATE, ERROR_MESSAGE, COMPLETED_TIME
            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                ROOT_TASK_NAME => '{dag_name}',
                RESULT_LIMIT => 100
            ))
            WHERE NAME != '{dag_name}'
            ORDER BY NAME
        """
            )
            .collect()
        )
    except Exception:
        # Compatibility fallback for accounts where
        # TASK_HISTORY(ROOT_TASK_NAME => ...) is not supported.
        # Enumerate child task names, then query TASK_HISTORY per child task.
        tasks = (
            session.sql(
                f"""
            SHOW TASKS LIKE '{dag_name}%'
        """
            )
            .collect()
        )
        child_names = [
            str(row["name"])
            for row in tasks
            if str(row["name"]) != dag_name
        ]
        result = []
        for child_name in sorted(child_names):
            hist = (
                session.sql(
                    f"""
                SELECT NAME, STATE, ERROR_MESSAGE, COMPLETED_TIME
                FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                    TASK_NAME => '{child_name}',
                    RESULT_LIMIT => 1
                ))
            """
                )
                .collect()
            )
            if len(hist) > 0:
                result.append(hist[0])
            else:
                # No run history yet: synthesize a scheduled placeholder.
                result.append(
                    {
                        "NAME": child_name,
                        "STATE": "SCHEDULED",
                        "ERROR_MESSAGE": None,
                        "COMPLETED_TIME": None,
                    }
                )

    chunks = []
    counts = {"total": 0, "succeeded": 0, "failed": 0, "running": 0, "scheduled": 0}

    for row in result:
        state = str(row["STATE"])
        chunks.append(
            {
                "name": str(row["NAME"]),
                "state": state,
                "error": (
                    str(row["ERROR_MESSAGE"]) if row["ERROR_MESSAGE"] else None
                ),
            }
        )
        counts["total"] += 1
        if state == "SUCCEEDED":
            counts["succeeded"] += 1
        elif state == "FAILED":
            counts["failed"] += 1
        elif state in ("EXECUTING", "RUNNING"):
            counts["running"] += 1
        else:
            counts["scheduled"] += 1

    return {"counts": counts, "chunks": chunks}


def cleanup_dag(session, dag_name: str) -> bool:
    """Drop the task graph after job completion.

    Dropping the root task cascades to all child tasks.

    Args:
        session: Snowpark session.
        dag_name: Name of the root task / DAG.

    Returns:
        True on success.
    """
    try:
        session.sql(f"ALTER TASK IF EXISTS {dag_name} SUSPEND").collect()
    except Exception:
        pass

    session.sql(f"DROP TASK IF EXISTS {dag_name}").collect()
    return True


def _build_job_spec(
    job_id: str,
    chunk_id: str,
    stage_path: str,
    image_uri: str,
    instance_family: str = "CPU_X64_S",
) -> str:
    """Generate a YAML service specification for a worker job container.

    The spec passes job metadata via environment variables that worker.R
    reads on startup.  Container resources are sized from *instance_family*
    using the same lookup table as the queue bridge.

    Args:
        job_id: UUID for the foreach job.
        chunk_id: Zero-padded chunk identifier (e.g. "001").
        stage_path: Full stage path to the job directory.
        image_uri: Docker image URI in the Snowflake image repo.
        instance_family: SPCS instance family for resource sizing.

    Returns:
        YAML string for the SPCS service specification.
    """
    res = INSTANCE_FAMILY_RESOURCES.get(
        instance_family.upper(),
        INSTANCE_FAMILY_RESOURCES["CPU_X64_S"],
    )
    cpu_req = res["cpu"]
    mem_req = res["memory"]
    cpu_lim = res["cpu_lim"]
    mem_lim = res["mem_lim"]

    stage_base = stage_path.split("/job_")[0]
    job_subdir = "job_" + stage_path.split("/job_")[1]
    spec = f"""
spec:
  containers:
  - name: r-worker
    image: {image_uri}
    env:
      JOB_ID: "{job_id}"
      CHUNK_ID: "{chunk_id}"
      STAGE_PATH: "{stage_path}"
      STAGE_MOUNT: "/data/stage"
      JOB_SUBDIR: "{job_subdir}"
    volumeMounts:
    - name: stage-vol
      mountPath: /data/stage
    resources:
      requests:
        cpu: {cpu_req}
        memory: {mem_req}
      limits:
        cpu: {cpu_lim}
        memory: {mem_lim}
  volumes:
  - name: stage-vol
    source: "{stage_base}"
    uid: 1000
    gid: 1000
"""
    return spec.strip()
