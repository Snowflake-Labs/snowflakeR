"""
doSnowflake Queue Bridge
========================
Python bridge for Hybrid Table queue operations used by doSnowflake
mode="queue". Handles queue DDL, work claiming, status polling,
and worker pool lifecycle.

Called from R via reticulate.
"""

import time
import uuid
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Queue DDL
# ---------------------------------------------------------------------------

def create_queue_table(session, fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE") -> bool:
    """Create the Hybrid Table queue if it does not exist.

    Args:
        session: Snowpark session.
        fqn: Fully-qualified table name (default CONFIG.DOSNOWFLAKE_QUEUE).

    Returns:
        True on success.
    """
    session.sql(f"""
        CREATE HYBRID TABLE IF NOT EXISTS {fqn} (
            QUEUE_ID     VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
            JOB_ID       VARCHAR NOT NULL,
            CHUNK_ID     VARCHAR NOT NULL,
            STATUS       VARCHAR DEFAULT 'PENDING',
            WORKER_ID    VARCHAR,
            CLAIMED_AT   TIMESTAMP_NTZ,
            COMPLETED_AT TIMESTAMP_NTZ,
            ERROR_MSG    VARCHAR,
            STAGE_PATH   VARCHAR NOT NULL,
            CREATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()
    return True


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------

def enqueue_chunks(
    session,
    job_id: str,
    n_chunks: int,
    stage_path: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> int:
    """Insert one queue row per chunk for a job.

    Args:
        session: Snowpark session.
        job_id: UUID for this foreach job.
        n_chunks: Number of chunks to enqueue.
        stage_path: Full stage path to the job directory.
        queue_fqn: Fully-qualified queue table name.

    Returns:
        Number of rows inserted.
    """
    if n_chunks == 0:
        return 0

    values = []
    for i in range(n_chunks):
        chunk_id = f"{i + 1:03d}"
        values.append(f"('{job_id}', '{chunk_id}', '{stage_path}')")

    sql = f"""
        INSERT INTO {queue_fqn} (JOB_ID, CHUNK_ID, STAGE_PATH)
        VALUES {', '.join(values)}
    """
    session.sql(sql).collect()
    return n_chunks


# ---------------------------------------------------------------------------
# Claim (used by workers)
# ---------------------------------------------------------------------------

def claim_work(
    session,
    job_id: str,
    worker_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> Optional[Dict[str, str]]:
    """Attempt to claim one PENDING chunk from the queue.

    Uses UPDATE ... WHERE STATUS='PENDING' LIMIT 1 for atomic claiming
    on Hybrid Tables. Returns the claimed row or None if queue is empty.

    Args:
        session: Snowpark session.
        job_id: UUID of the job to claim from (or '*' for any job).
        worker_id: Identifier for this worker.
        queue_fqn: Fully-qualified queue table name.

    Returns:
        Dict with QUEUE_ID, JOB_ID, CHUNK_ID, STAGE_PATH or None.
    """
    job_filter = "" if job_id == "*" else f"AND JOB_ID = '{job_id}'"

    claim_marker = f"{worker_id}_{uuid.uuid4().hex[:8]}"

    session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'RUNNING',
            WORKER_ID = '{claim_marker}',
            CLAIMED_AT = CURRENT_TIMESTAMP()
        WHERE QUEUE_ID = (
            SELECT QUEUE_ID FROM {queue_fqn}
            WHERE STATUS = 'PENDING' {job_filter}
            ORDER BY CREATED_AT
            LIMIT 1
        )
    """).collect()

    result = session.sql(f"""
        SELECT QUEUE_ID, JOB_ID, CHUNK_ID, STAGE_PATH
        FROM {queue_fqn}
        WHERE WORKER_ID = '{claim_marker}' AND STATUS = 'RUNNING'
        LIMIT 1
    """).collect()

    if len(result) == 0:
        return None

    row = result[0]
    return {
        "queue_id": str(row["QUEUE_ID"]),
        "job_id": str(row["JOB_ID"]),
        "chunk_id": str(row["CHUNK_ID"]),
        "stage_path": str(row["STAGE_PATH"]),
    }


# ---------------------------------------------------------------------------
# Status update (used by workers)
# ---------------------------------------------------------------------------

def mark_done(
    session,
    queue_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> bool:
    """Mark a queue item as DONE."""
    session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'DONE', COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE QUEUE_ID = '{queue_id}'
    """).collect()
    return True


def mark_failed(
    session,
    queue_id: str,
    error_msg: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> bool:
    """Mark a queue item as FAILED with an error message."""
    safe_msg = error_msg.replace("'", "''")[:1000]
    session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'FAILED',
            COMPLETED_AT = CURRENT_TIMESTAMP(),
            ERROR_MSG = '{safe_msg}'
        WHERE QUEUE_ID = '{queue_id}'
    """).collect()
    return True


# ---------------------------------------------------------------------------
# Polling (used by driver)
# ---------------------------------------------------------------------------

def poll_job_status(
    session,
    job_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> Dict[str, Any]:
    """Get status counts for a job.

    Returns:
        Dict with total, pending, running, done, failed counts.
    """
    result = session.sql(f"""
        SELECT STATUS, COUNT(*) AS CNT
        FROM {queue_fqn}
        WHERE JOB_ID = '{job_id}'
        GROUP BY STATUS
    """).collect()

    counts = {"total": 0, "pending": 0, "running": 0, "done": 0, "failed": 0}
    for row in result:
        status = str(row["STATUS"]).lower()
        cnt = int(row["CNT"])
        counts["total"] += cnt
        if status in counts:
            counts[status] = cnt

    return counts


def poll_until_complete(
    session,
    job_id: str,
    timeout_sec: int = 1800,
    poll_sec: int = 5,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
    stale_timeout_sec: int = 600,
    service_name: str = "",
    compute_pool: str = "",
    image_uri: str = "",
    n_workers: int = 0,
) -> Dict[str, Any]:
    """Poll until all chunks for a job are DONE or FAILED.

    Implements two levels of fault tolerance:
    1. Timestamp-based: requeues RUNNING items older than stale_timeout_sec.
    2. Worker-liveness: if all workers have exited and RUNNING items remain,
       requeues immediately and optionally relaunches ephemeral workers.

    Returns:
        Final status counts.
    """
    start = time.time()
    consecutive_stuck = 0
    last_progress_done = 0

    while True:
        elapsed = time.time() - start
        if elapsed > timeout_sec:
            return {"error": "timeout", **poll_job_status(session, job_id, queue_fqn)}

        requeued = _requeue_stale(session, job_id, stale_timeout_sec, queue_fqn)

        status = poll_job_status(session, job_id, queue_fqn)

        if status["pending"] == 0 and status["running"] == 0:
            return status

        # Detect orphaned RUNNING parcels when workers may be dead
        if status["running"] > 0 and status["pending"] == 0:
            if status["done"] == last_progress_done:
                consecutive_stuck += 1
            else:
                consecutive_stuck = 0
                last_progress_done = status["done"]

            # After several stuck polls, check if workers are alive
            if consecutive_stuck >= 3:
                workers_alive = _check_workers_alive(session, service_name)
                if not workers_alive:
                    orphaned = _force_requeue_running(
                        session, job_id, queue_fqn
                    )
                    if orphaned > 0 and n_workers > 0 and compute_pool and image_uri:
                        _relaunch_workers(
                            session, compute_pool, image_uri,
                            job_id, queue_fqn, min(n_workers, orphaned),
                        )
                    consecutive_stuck = 0
        else:
            consecutive_stuck = 0
            last_progress_done = status["done"]

        time.sleep(poll_sec)


def _requeue_stale(
    session,
    job_id: str,
    stale_timeout_sec: int,
    queue_fqn: str,
) -> int:
    """Mark RUNNING items older than stale_timeout_sec back to PENDING."""
    result = session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'PENDING', WORKER_ID = NULL, CLAIMED_AT = NULL
        WHERE JOB_ID = '{job_id}'
          AND STATUS = 'RUNNING'
          AND CLAIMED_AT < TIMESTAMPADD(SECOND, -{stale_timeout_sec}, CURRENT_TIMESTAMP())
    """).collect()
    return _rows_affected(result)


def _force_requeue_running(session, job_id: str, queue_fqn: str) -> int:
    """Immediately requeue all RUNNING items (workers confirmed dead)."""
    result = session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'PENDING', WORKER_ID = NULL, CLAIMED_AT = NULL
        WHERE JOB_ID = '{job_id}' AND STATUS = 'RUNNING'
    """).collect()
    return _rows_affected(result)


def _check_workers_alive(session, service_name: str) -> bool:
    """Check if the SPCS service/job still has running instances."""
    if not service_name:
        return True  # can't check, assume alive
    try:
        result = session.sql(
            f"SELECT SYSTEM$GET_SERVICE_STATUS('{service_name}')"
        ).collect()
        status_str = str(result[0][0]) if result else "[]"
        if status_str == "[]" or status_str == "":
            return False
        import json
        instances = json.loads(status_str)
        return any(
            inst.get("status") in ("READY", "PENDING", "RUNNING")
            for inst in instances
        ) if instances else False
    except Exception:
        return True  # on error, don't force requeue


def _relaunch_workers(
    session,
    compute_pool: str,
    image_uri: str,
    job_id: str,
    queue_fqn: str,
    n_workers: int,
) -> None:
    """Launch replacement ephemeral workers for orphaned parcels."""
    try:
        create_ephemeral_workers(
            session, compute_pool, image_uri,
            job_id, "", n_workers, queue_fqn,
        )
    except Exception:
        pass  # best effort


def _rows_affected(result) -> int:
    """Extract rows affected from a Snowpark UPDATE result."""
    try:
        if result and len(result) > 0:
            row = result[0]
            if hasattr(row, "__getitem__"):
                return int(row[0])
        return 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup_job(
    session,
    job_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> int:
    """Delete all queue rows for a completed job."""
    session.sql(f"""
        DELETE FROM {queue_fqn} WHERE JOB_ID = '{job_id}'
    """).collect()
    return 0


# ---------------------------------------------------------------------------
# Worker pool lifecycle
# ---------------------------------------------------------------------------

def create_worker_pool(
    session,
    service_name: str,
    compute_pool: str,
    image_uri: str,
    replicas: int = 2,
    job_id: str = "*",
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
    stage_path: str = "",
    cpu_request: int = 2,
    memory_request: str = "4Gi",
) -> str:
    """Create a long-running SPCS service with queue-pulling workers.

    Args:
        session: Snowpark session.
        service_name: Name for the SPCS service.
        compute_pool: Compute pool to run on.
        image_uri: Docker image URI with worker_queue.R.
        replicas: Number of worker instances.
        job_id: Job to process ('*' for any).
        queue_fqn: Fully-qualified queue table name.
        stage_path: Stage path for shared job data.

    Returns:
        Service name.
    """
    db = session.get_current_database()
    schema = session.get_current_schema()

    spec = f"""
spec:
  containers:
  - name: r-worker
    image: {image_uri}
    env:
      WORKER_MODE: "queue"
      JOB_ID: "{job_id}"
      QUEUE_FQN: "{queue_fqn}"
      STAGE_PATH: "{stage_path}"
    resources:
      requests:
        cpu: {cpu_request}
        memory: {memory_request}
      limits:
        cpu: {cpu_request * 2}
        memory: {memory_request.replace('Gi', '')}
    """.rstrip()

    # Add volume mount if stage_path provided
    if stage_path:
        stage_root = stage_path.split("/job_")[0] if "/job_" in stage_path else stage_path
        spec += f"""
  volumes:
  - name: stage-vol
    source: "{stage_root}"
    uid: 1000
    gid: 1000
    """

    session.sql(f"""
        CREATE SERVICE IF NOT EXISTS {service_name}
        IN COMPUTE POOL {compute_pool}
        MIN_INSTANCES = {replicas}
        MAX_INSTANCES = {replicas}
        FROM SPECIFICATION $${spec}$$
    """).collect()

    return service_name


INSTANCE_FAMILY_RESOURCES = {
    "CPU_X64_XS": {"cpu": 1, "memory": "4Gi"},
    "CPU_X64_S":  {"cpu": 3, "memory": "12Gi"},
    "CPU_X64_M":  {"cpu": 6, "memory": "24Gi"},
    "CPU_X64_L":  {"cpu": 12, "memory": "48Gi"},
    "CPU_X64_XL": {"cpu": 24, "memory": "96Gi"},
}


def _get_resource_spec(instance_family: str) -> Dict[str, Any]:
    """Return appropriate cpu/memory requests for an instance family."""
    resources = INSTANCE_FAMILY_RESOURCES.get(
        instance_family.upper(),
        INSTANCE_FAMILY_RESOURCES["CPU_X64_S"],
    )
    cpu = resources["cpu"]
    mem = resources["memory"]
    mem_limit = f"{int(mem.replace('Gi', '')) * 2}Gi"
    return {
        "cpu_request": cpu,
        "memory_request": mem,
        "cpu_limit": cpu * 2,
        "memory_limit": mem_limit,
    }


def create_ephemeral_workers(
    session,
    compute_pool: str,
    image_uri: str,
    job_id: str,
    stage_path: str,
    n_workers: int = 4,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
    instance_family: str = "CPU_X64_S",
    warehouse: str = "",
) -> str:
    """Launch ephemeral EXECUTE JOB SERVICE workers that pull from queue.

    Resource requests are automatically sized based on instance_family.
    Each worker claims chunks until the queue is empty, then exits.
    The container runs ``worker_queue.R`` (generic doSnowflake queue
    worker) instead of the image's default entrypoint.

    Returns:
        Job service name.
    """
    res = _get_resource_spec(instance_family)
    stage_root = stage_path.split("/job_")[0] if "/job_" in stage_path else stage_path

    if not warehouse:
        try:
            wh_rows = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()
            warehouse = wh_rows[0]["WH"] if wh_rows and wh_rows[0]["WH"] else ""
        except Exception:
            warehouse = ""

    wh_env = f'\n      SNOWFLAKE_WAREHOUSE: "{warehouse}"' if warehouse else ""

    spec = f"""
spec:
  containers:
  - name: r-worker
    image: {image_uri}
    command:
    - bash
    - -c
    - "if [ -f /stage/worker_queue.R ]; then Rscript /stage/worker_queue.R; else Rscript /app/worker_queue.R; fi"
    env:
      WORKER_MODE: "ephemeral"
      JOB_ID: "{job_id}"
      QUEUE_FQN: "{queue_fqn}"
      STAGE_PATH: "{stage_path}"
      STAGE_MOUNT: "/stage"{wh_env}
    volumeMounts:
    - name: stage-vol
      mountPath: /stage
    resources:
      requests:
        cpu: {res['cpu_request']}
        memory: {res['memory_request']}
      limits:
        cpu: {res['cpu_limit']}
        memory: {res['memory_limit']}
  volumes:
  - name: stage-vol
    source: "{stage_root}"
    uid: 1000
    gid: 1000
    """.strip()

    ejs_sql = f"""
        EXECUTE JOB SERVICE
        IN COMPUTE POOL {compute_pool}
        FROM SPECIFICATION $${spec}$$
        REPLICAS = {n_workers}
    """
    import sys
    print(f"[doSnowflake] Launching {n_workers} workers in {compute_pool}", file=sys.stderr)
    session.sql(ejs_sql).collect()

    return f"ephemeral_{job_id[:8]}"


def get_pool_status(session, service_name: str) -> Dict[str, Any]:
    """Get status of a worker pool service."""
    try:
        result = session.sql(f"DESCRIBE SERVICE {service_name}").collect()
        return {"name": service_name, "status": "RUNNING", "instances": len(result)}
    except Exception as e:
        return {"name": service_name, "status": "NOT_FOUND", "error": str(e)}


def stop_worker_pool(session, service_name: str) -> bool:
    """Drop a worker pool service."""
    session.sql(f"DROP SERVICE IF EXISTS {service_name}").collect()
    return True


def scale_worker_pool(
    session,
    service_name: str,
    replicas: int,
) -> str:
    """Scale a persistent worker pool to a new replica count."""
    session.sql(f"""
        ALTER SERVICE {service_name}
        SET MIN_INSTANCES = {replicas} MAX_INSTANCES = {replicas}
    """).collect()
    return service_name


def wait_for_workers_ready(
    session,
    n_expected: int,
    timeout_sec: int = 300,
    poll_sec: int = 5,
) -> bool:
    """Wait until n_expected SPCS job instances report READY status.

    Polls SHOW SERVICES / SYSTEM$GET_SERVICE_STATUS to check instance
    readiness. Used for pre-warming workers before enqueuing work.
    """
    import json as _json

    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            result = session.sql(
                "SELECT SYSTEM$GET_SERVICE_STATUS('*')"
            ).collect()
            if result:
                status_str = str(result[0][0])
                instances = _json.loads(status_str) if status_str else []
                ready_count = sum(
                    1 for inst in instances
                    if inst.get("status") == "READY"
                )
                if ready_count >= n_expected:
                    return True
        except Exception:
            pass
        time.sleep(poll_sec)

    return False


def estimate_throughput(
    session,
    job_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> Dict[str, Any]:
    """Estimate throughput and ETA for a running queue job."""
    result = session.sql(f"""
        SELECT
            COUNT(CASE WHEN STATUS = 'DONE' THEN 1 END) AS DONE,
            COUNT(CASE WHEN STATUS IN ('PENDING', 'RUNNING') THEN 1 END) AS REMAINING,
            MIN(CASE WHEN STATUS = 'DONE' THEN COMPLETED_AT END) AS FIRST_DONE,
            MAX(CASE WHEN STATUS = 'DONE' THEN COMPLETED_AT END) AS LAST_DONE,
            DATEDIFF('SECOND', FIRST_DONE, LAST_DONE) AS ELAPSED_SEC
        FROM {queue_fqn}
        WHERE JOB_ID = '{job_id}'
    """).collect()

    if not result:
        return {"throughput_per_min": 0.0, "eta_minutes": None}

    row = result[0]
    done = int(row["DONE"]) if row["DONE"] else 0
    remaining = int(row["REMAINING"]) if row["REMAINING"] else 0
    elapsed_sec = float(row["ELAPSED_SEC"]) if row["ELAPSED_SEC"] else 0

    if done == 0 or elapsed_sec == 0:
        return {"throughput_per_min": 0.0, "eta_minutes": None}

    throughput = done / elapsed_sec * 60
    eta = remaining / throughput if throughput > 0 else None

    return {"throughput_per_min": round(throughput, 1), "eta_minutes": round(eta, 1) if eta else None}
