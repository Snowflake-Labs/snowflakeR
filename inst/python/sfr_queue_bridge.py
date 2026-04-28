"""
doSnowflake Queue Bridge
========================
Python bridge for Hybrid Table queue operations used by doSnowflake
mode="queue". Handles queue DDL, work claiming, status polling,
and worker pool lifecycle.

Called from R via reticulate.
"""

import time
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
            LEASE_UNTIL  TIMESTAMP_NTZ,
            ATTEMPTS     NUMBER DEFAULT 0,
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
    lease_minutes: int = 10,
) -> Optional[Dict[str, str]]:
    """Claim one PENDING (or expired-lease RUNNING) chunk via compare-and-swap.

    Two-step CAS prevents silent race losses that caused stuck PENDING chunks:
      1. SELECT a candidate row (PENDING or expired LEASE_UNTIL)
      2. UPDATE with WHERE re-checking claimability
      3. Check rows_affected: 0 means race lost -> return "RETRY"

    Args:
        session: Snowpark session.
        job_id: UUID of the job to claim from (or '*' for any job).
        worker_id: Identifier for this worker.
        queue_fqn: Fully-qualified queue table name.
        lease_minutes: Lease duration in minutes.

    Returns:
        Dict with QUEUE_ID, JOB_ID, CHUNK_ID, STAGE_PATH; or
        "RETRY" if race lost; or None if queue is empty.
    """
    job_filter = "" if job_id == "*" else f"AND JOB_ID = '{job_id}'"
    safe_wid = worker_id.replace("'", "''")

    candidate = session.sql(f"""
        SELECT QUEUE_ID FROM {queue_fqn}
        WHERE (STATUS = 'PENDING'
               OR (STATUS = 'RUNNING'
                   AND LEASE_UNTIL IS NOT NULL
                   AND LEASE_UNTIL < CURRENT_TIMESTAMP()))
          {job_filter}
        ORDER BY CREATED_AT
        LIMIT 1
    """).collect()

    if len(candidate) == 0:
        return None

    cand_id = str(candidate[0]["QUEUE_ID"])

    result = session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS      = 'RUNNING',
            WORKER_ID   = '{safe_wid}',
            CLAIMED_AT  = CURRENT_TIMESTAMP(),
            LEASE_UNTIL = DATEADD('minute', {lease_minutes}, CURRENT_TIMESTAMP()),
            ATTEMPTS    = NVL(ATTEMPTS, 0) + 1
        WHERE QUEUE_ID = '{cand_id}'
          AND (STATUS = 'PENDING'
               OR (STATUS = 'RUNNING'
                   AND LEASE_UNTIL IS NOT NULL
                   AND LEASE_UNTIL < CURRENT_TIMESTAMP()))
    """).collect()

    n_updated = _rows_affected(result)
    if n_updated == 0:
        return "RETRY"

    row_result = session.sql(f"""
        SELECT QUEUE_ID, JOB_ID, CHUNK_ID, STAGE_PATH
        FROM {queue_fqn} WHERE QUEUE_ID = '{cand_id}'
    """).collect()

    if len(row_result) == 0:
        return None

    row = row_result[0]
    return {
        "queue_id": str(row["QUEUE_ID"]),
        "job_id": str(row["JOB_ID"]),
        "chunk_id": str(row["CHUNK_ID"]),
        "stage_path": str(row["STAGE_PATH"]),
    }


def renew_lease(
    session,
    queue_id: str,
    worker_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
    lease_minutes: int = 10,
) -> bool:
    """Extend the lease on a claimed queue item."""
    safe_wid = worker_id.replace("'", "''")
    session.sql(f"""
        UPDATE {queue_fqn}
        SET LEASE_UNTIL = DATEADD('minute', {lease_minutes}, CURRENT_TIMESTAMP())
        WHERE QUEUE_ID = '{queue_id}'
          AND WORKER_ID = '{safe_wid}'
          AND STATUS = 'RUNNING'
    """).collect()
    return True


# ---------------------------------------------------------------------------
# Status update (used by workers)
# ---------------------------------------------------------------------------

def mark_done(
    session,
    queue_id: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> bool:
    """Mark a queue item as DONE and release its lease."""
    session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'DONE',
            COMPLETED_AT = CURRENT_TIMESTAMP(),
            LEASE_UNTIL = NULL
        WHERE QUEUE_ID = '{queue_id}'
    """).collect()
    return True


def mark_failed(
    session,
    queue_id: str,
    error_msg: str,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
) -> bool:
    """Mark a queue item as FAILED with an error message and release its lease."""
    safe_msg = error_msg.replace("'", "''")[:1000]
    session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'FAILED',
            COMPLETED_AT = CURRENT_TIMESTAMP(),
            LEASE_UNTIL = NULL,
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
    """Requeue RUNNING items that are stale (old CLAIMED_AT) or have expired leases."""
    result = session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'PENDING', WORKER_ID = NULL,
            CLAIMED_AT = NULL, LEASE_UNTIL = NULL
        WHERE JOB_ID = '{job_id}'
          AND STATUS = 'RUNNING'
          AND (CLAIMED_AT < TIMESTAMPADD(SECOND, -{stale_timeout_sec}, CURRENT_TIMESTAMP())
               OR (LEASE_UNTIL IS NOT NULL
                   AND LEASE_UNTIL < CURRENT_TIMESTAMP()))
    """).collect()
    return _rows_affected(result)


def _force_requeue_running(session, job_id: str, queue_fqn: str) -> int:
    """Immediately requeue all RUNNING items (workers confirmed dead)."""
    result = session.sql(f"""
        UPDATE {queue_fqn}
        SET STATUS = 'PENDING', WORKER_ID = NULL,
            CLAIMED_AT = NULL, LEASE_UNTIL = NULL
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


# Node capacity (from SHOW COMPUTE POOL INSTANCE FAMILIES):
NODE_CAPACITY = {
    "CPU_X64_XS": {"cpu": 1,  "mem_gib": 6},
    "CPU_X64_S":  {"cpu": 3,  "mem_gib": 13},
    "CPU_X64_M":  {"cpu": 6,  "mem_gib": 28},
    "CPU_X64_SL": {"cpu": 14, "mem_gib": 54},
    "CPU_X64_L":  {"cpu": 28, "mem_gib": 116},
}


def _get_resource_spec(
    instance_family: str,
    containers_per_node: int = 1,
) -> Dict[str, Any]:
    """Compute container cpu/memory request+limit for an instance family.

    Divides node capacity by *containers_per_node* so SPCS packs exactly
    that many containers per node.  With containers_per_node=1 (default),
    each worker gets a dedicated node — guaranteeing horizontal scaling.

    worker.R uses ``mclapply(mc.cores = detectCores() - 1)`` where
    ``detectCores()`` returns the container's CPU **limit**.  Setting
    the limit to the per-container CPU share ensures mclapply forks the
    right number of processes without user math.
    """
    node = NODE_CAPACITY.get(
        instance_family.upper(),
        NODE_CAPACITY["CPU_X64_S"],
    )
    containers_per_node = max(1, containers_per_node)
    cpu_share = node["cpu"] // containers_per_node
    mem_share = node["mem_gib"] // containers_per_node
    return {
        "cpu_request": max(1, cpu_share - 1),
        "memory_request": f"{max(2, mem_share - 2)}Gi",
        "cpu_limit": max(1, cpu_share),
        "memory_limit": f"{mem_share}Gi",
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
    containers_per_node: int = 1,
) -> str:
    """Launch ephemeral EXECUTE JOB SERVICE workers that pull from queue.

    Resource requests are automatically sized based on instance_family
    and containers_per_node.  With containers_per_node=1 (default), each
    worker gets a dedicated node for maximum per-worker throughput.

    Returns:
        Job service name.
    """
    res = _get_resource_spec(instance_family, containers_per_node)
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
      STAGE_MOUNT: "/stage"
      LEASE_MINUTES: "10"{wh_env}
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


# ---------------------------------------------------------------------------
# Time-boxed dynamic queue feeder (benchmark mode)
# ---------------------------------------------------------------------------

def run_time_boxed_queue(
    session,
    compute_pool: str,
    image_uri: str,
    job_id: str,
    stage_path: str,
    total_chunks: int,
    n_workers: int,
    time_limit_sec: float,
    queue_fqn: str = "CONFIG.DOSNOWFLAKE_QUEUE",
    instance_family: str = "CPU_X64_S",
    low_water: int = 4,
    batch_size: int = 4,
    skus_per_chunk: int = 0,
    poll_sec: int = 3,
    max_empty_polls: int = 30,
    containers_per_node: int = 1,
    drain_multiplier: float = 1.5,
) -> Dict[str, Any]:
    """Run queue workers with a time-boxed continuous feeder.

    Pre-serialized chunks (chunk_001 .. chunk_{total_chunks}) must already
    exist on *stage_path*.  This function:

    1. Launches *n_workers* ephemeral workers (with a high MAX_EMPTY_POLLS
       so they survive brief queue-empty gaps).
    2. Seeds an initial batch of queue rows.
    3. Continuously feeds more rows whenever PENDING drops below *low_water*,
       until *time_limit_sec* elapses or all chunks are enqueued.
    4. Waits for workers to drain remaining queue rows, up to
       *time_limit_sec * drain_multiplier* total elapsed.
    5. Returns stats: chunks processed, SKUs, elapsed time, throughput.

    Args:
        session: Snowpark session.
        compute_pool: SPCS compute pool name.
        image_uri: Docker image URI for workers.
        job_id: UUID for this benchmark job.
        stage_path: Full stage path containing chunk files.
        total_chunks: Number of pre-serialized chunks available.
        n_workers: Number of ephemeral worker replicas.
        time_limit_sec: Maximum seconds for the feeder to keep adding work.
        queue_fqn: Fully-qualified Hybrid Table name.
        instance_family: Instance family for worker resource sizing.
        low_water: Enqueue more when PENDING count drops below this.
        batch_size: Number of chunks to enqueue per batch.
        skus_per_chunk: SKUs per chunk (for reporting only).
        poll_sec: Seconds between feeder/poll cycles.
        max_empty_polls: MAX_EMPTY_POLLS env var for workers.
        containers_per_node: Workers packed per node (1 = dedicated node).
        drain_multiplier: Total elapsed time cap as a multiple of
            time_limit_sec (default 1.5 = 50% extra for drain).

    Returns:
        Dict with chunks_fed, chunks_done, chunks_failed, skus_processed,
        elapsed_sec, skus_per_sec, feeder_stopped_at.
    """
    import sys

    res = _get_resource_spec(instance_family, containers_per_node)
    stage_root = stage_path.split("/job_")[0] if "/job_" in stage_path else stage_path

    # Resolve warehouse
    warehouse = ""
    try:
        wh_rows = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()
        warehouse = wh_rows[0]["WH"] if wh_rows and wh_rows[0]["WH"] else ""
    except Exception:
        pass
    wh_env = f'\n      SNOWFLAKE_WAREHOUSE: "{warehouse}"' if warehouse else ""

    # --- 1. Launch workers with high MAX_EMPTY_POLLS ----
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
      STAGE_MOUNT: "/stage"
      LEASE_MINUTES: "10"
      MAX_EMPTY_POLLS: "{max_empty_polls}"{wh_env}
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

    # --- 2. Seed initial batch BEFORE launching workers ---
    # Workers may start on a warm pool before seeding completes,
    # and a cold pool gives us time to seed while nodes provision.
    chunks_fed = 0
    initial_batch = min(n_workers * 2, total_chunks)

    def _feed_batch(n):
        nonlocal chunks_fed
        to_feed = min(n, total_chunks - chunks_fed)
        if to_feed <= 0:
            return 0
        values = []
        for _ in range(to_feed):
            chunks_fed += 1
            cid = f"{chunks_fed:03d}"
            values.append(f"('{job_id}', '{cid}', '{stage_path}')")
        session.sql(
            f"INSERT INTO {queue_fqn} (JOB_ID, CHUNK_ID, STAGE_PATH) "
            f"VALUES {', '.join(values)}"
        ).collect()
        return to_feed

    fed = _feed_batch(initial_batch)
    print(f"[benchmark] Seeded {fed} chunks into queue", flush=True)

    print(
        f"[benchmark] Launching {n_workers} workers in {compute_pool} "
        f"(MAX_EMPTY_POLLS={max_empty_polls})",
        flush=True,
    )
    # EJS .collect() blocks until the job completes, which would deadlock
    # the feeder loop.  Submit asynchronously via the underlying cursor.
    import threading
    def _submit_ejs():
        try:
            session.sql(ejs_sql).collect()
        except Exception as e:
            print(f"[benchmark] EJS error: {e}", flush=True)
    _ejs_thread = threading.Thread(target=_submit_ejs, daemon=True)
    _ejs_thread.start()
    print("[benchmark] Workers submitted (async)", flush=True)

    # --- 3. Feeder loop ---
    drain_deadline = time_limit_sec * max(1.0, drain_multiplier)
    start = time.time()
    feeder_stopped_at = None
    drain_timeout_hit = False

    while True:
        elapsed = time.time() - start

        status = poll_job_status(session, job_id, queue_fqn)
        pending = status.get("pending", 0)
        done = status.get("done", 0)
        failed = status.get("failed", 0)
        running = status.get("running", 0)

        print(
            f"[benchmark] {elapsed:.0f}s | "
            f"done={done} running={running} pending={pending} "
            f"fed={chunks_fed}/{total_chunks}",
            flush=True,
        )

        if feeder_stopped_at is None:
            if elapsed >= time_limit_sec:
                feeder_stopped_at = elapsed
                print(
                    f"[benchmark] Feeder stopped at {elapsed:.1f}s "
                    f"(limit={time_limit_sec:.0f}s). "
                    f"Chunks fed: {chunks_fed}, done: {done}. "
                    f"Drain deadline: {drain_deadline:.0f}s "
                    f"({drain_multiplier:.1f}x)",
                    flush=True,
                )
            elif pending < low_water and chunks_fed < total_chunks:
                fed = _feed_batch(batch_size)
                if fed > 0:
                    print(
                        f"[benchmark] +{fed} chunks (total fed={chunks_fed})",
                        flush=True,
                    )

        all_fed = chunks_fed >= total_chunks
        all_done = (done + failed) >= chunks_fed and pending == 0 and running == 0

        if all_fed and all_done:
            if feeder_stopped_at is None:
                feeder_stopped_at = elapsed
            print(
                f"[benchmark] All {chunks_fed} chunks complete at {elapsed:.1f}s",
                flush=True,
            )
            break

        if feeder_stopped_at is not None and pending == 0 and running == 0:
            break

        if feeder_stopped_at is not None and elapsed > drain_deadline:
            drain_timeout_hit = True
            print(
                f"[benchmark] Drain timeout at {elapsed:.1f}s "
                f"(limit={time_limit_sec:.0f}s * {drain_multiplier:.1f}x "
                f"= {drain_deadline:.0f}s). "
                f"Stopping — {done} done, {running} in-flight lost.",
                flush=True,
            )
            break

        time.sleep(poll_sec)

    total_elapsed = time.time() - start

    # If drain timed out, attempt to drop the worker service to release
    # compute nodes.  EJS services have auto-generated names, so we search
    # SHOW SERVICES for a matching job.
    if drain_timeout_hit:
        try:
            svc_rows = session.sql(
                f"SHOW SERVICES IN COMPUTE POOL {compute_pool}"
            ).collect()
            for row in svc_rows:
                svc_name = row["name"] if "name" in row.as_dict() else ""
                if not svc_name:
                    continue
                try:
                    status_json = session.sql(
                        f"SELECT SYSTEM$GET_SERVICE_STATUS('{svc_name}')"
                    ).collect()
                    if job_id in str(status_json):
                        session.sql(
                            f"DROP SERVICE IF EXISTS {svc_name}"
                        ).collect()
                        print(
                            f"[benchmark] Dropped worker service {svc_name}",
                            flush=True,
                        )
                        break
                except Exception:
                    pass
        except Exception as e:
            print(f"[benchmark] Could not drop workers: {e}", flush=True)

    final = poll_job_status(session, job_id, queue_fqn)
    total_done = final.get("done", 0)
    total_failed = final.get("failed", 0)
    total_skus = total_done * skus_per_chunk if skus_per_chunk else 0
    skus_per_sec = total_skus / total_elapsed if total_elapsed > 0 and total_skus else 0

    result = {
        "chunks_fed": chunks_fed,
        "chunks_done": total_done,
        "chunks_failed": total_failed,
        "skus_per_chunk": skus_per_chunk,
        "skus_processed": total_skus,
        "elapsed_sec": round(total_elapsed, 1),
        "feeder_stopped_at": round(feeder_stopped_at, 1) if feeder_stopped_at else None,
        "skus_per_sec": round(skus_per_sec, 1),
        "drain_timeout": drain_timeout_hit,
    }
    print(f"[benchmark] Queue result: {result}", flush=True)
    return result
