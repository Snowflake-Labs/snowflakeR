"""
doSnowflake Benchmark Monitor — Streamlit in Snowflake dashboard.

Provides live visibility into tasks-vs-queue benchmark runs:
  Tab 1: Compute pool nodes, services, and job count
  Tab 2: Tasks phase — DAG chunk status, progress, throughput
  Tab 3: Queue phase — chunk claims, progress bar, per-chunk timing, cumulative chart

Deploy with:
  PUT file://streamlit_benchmark_monitor.py
    @SNOWFLAKER_DEMO_DB.SOURCE_DATA.DOSNOWFLAKE_STAGE/streamlit/
    AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
  CREATE OR REPLACE STREAMLIT SNOWFLAKER_DEMO_DB.CONFIG.BENCHMARK_MONITOR
    ROOT_LOCATION = '@SNOWFLAKER_DEMO_DB.SOURCE_DATA.DOSNOWFLAKE_STAGE/streamlit/'
    MAIN_FILE = 'streamlit_benchmark_monitor.py'
    QUERY_WAREHOUSE = 'SNOWFLAKER_DEMO_WH';
"""

from __future__ import annotations

import time

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    session = Session.builder.config("connection_name", "default").create()

POOL_NAME = "SNOWFLAKER_DEMO_POOL"
QUEUE_FQN = "SNOWFLAKER_DEMO_DB.CONFIG.DOSNOWFLAKE_QUEUE"
DATABASE = "SNOWFLAKER_DEMO_DB"
INSTANCE_FAMILY = "CPU_X64_L"
STAGE_FQN = f"{DATABASE}.SOURCE_DATA.DOSNOWFLAKE_STAGE"


def _rerun():
    """Compatible with both old (experimental_rerun) and new (rerun) Streamlit."""
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


st.set_page_config(page_title="Benchmark Monitor", layout="wide")

# ── Snowflake-branded light theme ──────────────────────────────────────
_SF_BLUE = "#29B5E8"
_SF_MID_BLUE = "#11567F"
_SF_STAR_BLUE = "#75CDD7"
_SF_GRAY = "#5B5B5B"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700&family=Montserrat:wght@800&display=swap');

/* Force light background everywhere */
html, body, [data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > .main {{
    background-color: #FFFFFF;
    color: {_SF_GRAY};
}}
[data-testid="stHeader"] {{
    background-color: #FFFFFF;
}}

/* Sidebar */
[data-testid="stSidebar"] {{
    background-color: {_SF_MID_BLUE};
}}
[data-testid="stSidebar"],
[data-testid="stSidebar"] *,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] .stMarkdown code,
[data-testid="stSidebar"] .stMarkdown strong,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stCheckbox label,
[data-testid="stSidebar"] .stCheckbox label span,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"],
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] p {{
    color: #FFFFFF !important;
}}
/* Sidebar code/backtick blocks */
[data-testid="stSidebar"] code {{
    color: {_SF_STAR_BLUE} !important;
    background-color: rgba(255, 255, 255, 0.15) !important;
}}
/* Selectbox value text (inside the closed dropdown) */
[data-testid="stSidebar"] [data-baseweb="select"] span {{
    color: #FFFFFF !important;
}}
/* Selectbox dropdown arrow */
[data-testid="stSidebar"] [data-baseweb="select"] svg {{
    fill: #FFFFFF !important;
}}
/* Selectbox input border */
[data-testid="stSidebar"] [data-baseweb="select"] > div {{
    border-color: rgba(255, 255, 255, 0.4) !important;
    background-color: rgba(255, 255, 255, 0.1) !important;
}}
/* Open dropdown menu stays readable (dark text on white) */
[data-baseweb="popover"] li,
[data-baseweb="menu"] li {{
    color: {_SF_GRAY} !important;
}}

/* Typography */
html, body, [class*="stMarkdown"], p, li, span, td, th, label,
[data-testid="stMetricValue"], [data-testid="stMetricLabel"] {{
    font-family: 'Lato', Arial, Helvetica, sans-serif !important;
}}
h1, h2, h3 {{
    font-family: 'Montserrat', Arial, sans-serif !important;
    color: {_SF_MID_BLUE} !important;
    font-weight: 800 !important;
}}
h1 {{ text-transform: uppercase; letter-spacing: 0.02em; }}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {{
    gap: 0px;
    border-bottom: 2px solid {_SF_BLUE};
}}
.stTabs [data-baseweb="tab"] {{
    font-family: 'Lato', Arial, sans-serif !important;
    font-weight: 700;
    color: {_SF_MID_BLUE};
    padding: 10px 24px;
}}
.stTabs [aria-selected="true"] {{
    background-color: {_SF_BLUE};
    color: #FFFFFF !important;
    border-radius: 6px 6px 0 0;
}}

/* Metric cards */
[data-testid="stMetric"] {{
    background: linear-gradient(135deg, {_SF_BLUE}10, {_SF_STAR_BLUE}18);
    border: 1px solid {_SF_BLUE}40;
    border-radius: 8px;
    padding: 12px 16px;
}}
[data-testid="stMetricValue"] {{
    color: {_SF_MID_BLUE} !important;
    font-weight: 700 !important;
}}
[data-testid="stMetricLabel"] {{
    color: {_SF_GRAY} !important;
}}

/* Progress bar */
.stProgress > div > div > div {{
    background-color: {_SF_BLUE} !important;
}}

/* Expanders */
[data-testid="stExpander"] {{
    border: 1px solid {_SF_BLUE}30;
    border-radius: 6px;
}}

/* Dataframes */
[data-testid="stDataFrame"] {{
    border: 1px solid {_SF_BLUE}25;
    border-radius: 6px;
}}

/* Buttons */
.stButton > button {{
    background-color: {_SF_BLUE};
    color: #FFFFFF;
    border: none;
    border-radius: 6px;
    font-family: 'Lato', Arial, sans-serif;
    font-weight: 700;
}}
.stButton > button:hover {{
    background-color: {_SF_MID_BLUE};
    color: #FFFFFF;
}}
</style>
""", unsafe_allow_html=True)

st.title("doSnowflake Benchmark Monitor")

auto_refresh = st.sidebar.checkbox("Auto-refresh (5 s)", value=True)
if st.sidebar.button("Refresh Now"):
    _rerun()
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Pool:** `{POOL_NAME}`")
st.sidebar.markdown(f"**Instance:** `{INSTANCE_FAMILY}`")
st.sidebar.markdown(f"**Database:** `{DATABASE}`")


def _safe_query(sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.caption(f"Query error: {e}")
        return pd.DataFrame()


def _naive_ts(ts):
    """Strip timezone from a pandas Timestamp to avoid tz-naive/aware mismatches."""
    if ts is None or pd.isna(ts):
        return ts
    ts = pd.Timestamp(ts)
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _utcnow():
    """Return current UTC time as a tz-naive Timestamp."""
    return pd.Timestamp.utcnow().tz_localize(None)


def _safe_int(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _col(df_row, name, default="?"):
    """Get column value trying exact, lower, upper, and title-case variants."""
    for variant in [name, name.lower(), name.upper(), name.title()]:
        try:
            v = df_row[variant]
            if v is not None and str(v).strip() != "":
                return v
        except (KeyError, IndexError):
            continue
    return default


tab_pool, tab_tasks, tab_queue = st.tabs(
    ["Compute Pool", "Tasks Phase", "Queue Phase"]
)

# ━━ Tab 1: Compute Pool ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_pool:
    st.header("Compute Pool Status")

    pool_df = _safe_query(f"SHOW COMPUTE POOLS LIKE '{POOL_NAME}'")

    if len(pool_df) > 0:
        p = pool_df.iloc[0]

        state = _col(p, "state")
        active = _safe_int(_col(p, "active_nodes", 0))
        target = _safe_int(_col(p, "target_nodes", 0))
        idle = _safe_int(_col(p, "idle_nodes", 0))
        jobs = _safe_int(_col(p, "num_jobs", 0))
        svcs = _safe_int(_col(p, "num_services", 0))

        cols = st.columns(6)
        cols[0].metric("State", state)
        cols[1].metric("Active Nodes", active)
        cols[2].metric("Target Nodes", target)
        cols[3].metric("Idle Nodes", idle)
        cols[4].metric("Jobs", jobs)
        cols[5].metric("Services", svcs)

        with st.expander("Raw pool data", expanded=False):
            st.dataframe(pool_df, use_container_width=True)
    else:
        st.warning(f"Compute pool `{POOL_NAME}` not found or no MONITOR privilege.")

    st.subheader("Services")
    try:
        svc_rows = session.sql(
            f"SHOW SERVICES IN COMPUTE POOL {POOL_NAME}"
        ).collect()
        if svc_rows:
            svc_data = []
            for r in svc_rows:
                row = {}
                for key in ("name", "status", "min_instances",
                            "max_instances", "compute_pool"):
                    for variant in (key, f'"{key}"', key.upper(),
                                    f'"{key.upper()}"'):
                        try:
                            row[key] = r[variant]
                            break
                        except (KeyError, IndexError, Exception):
                            continue
                svc_data.append(row)
            svc_df = pd.DataFrame(svc_data)
            st.dataframe(svc_df, use_container_width=True)
        else:
            st.caption("No services running.")
    except Exception as e:
        st.caption(f"Services query error: {e}")

# ━━ Tab 2: Tasks Phase ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_tasks:
    st.header("Tasks Phase (DAG)")

    root_df = _safe_query(f"""
        SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
        FROM TABLE({DATABASE}.INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 50))
        WHERE NAME LIKE 'DOSNOWFLAKE%'
          AND NAME NOT LIKE '%CHUNK%'
        ORDER BY SCHEDULED_TIME DESC
    """)

    if len(root_df) > 0:
        latest_dag = str(root_df.iloc[0]["NAME"])
        st.caption(f"Latest DAG: `{latest_dag}`")

        child_df = _safe_query(f"""
            SELECT NAME, STATE,
                   SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE,
                   DATEDIFF('second', SCHEDULED_TIME, COALESCE(COMPLETED_TIME, CURRENT_TIMESTAMP())) AS DURATION_SEC
            FROM TABLE({DATABASE}.INFORMATION_SCHEMA.TASK_HISTORY(
                RESULT_LIMIT => 200
            ))
            WHERE NAME LIKE '{latest_dag}$%'
            ORDER BY NAME
        """)

        if len(child_df) > 0:
            total = len(child_df)
            succeeded = len(child_df[child_df["STATE"] == "SUCCEEDED"])
            failed = len(child_df[child_df["STATE"] == "FAILED"])
            executing = len(child_df[child_df["STATE"].isin(
                ["EXECUTING", "RUNNING"])])
            scheduled = total - succeeded - failed - executing

            cols = st.columns(5)
            cols[0].metric("Total Chunks", total)
            cols[1].metric("Succeeded", succeeded)
            cols[2].metric("Executing", executing)
            cols[3].metric("Scheduled", scheduled)
            cols[4].metric("Failed", failed)

            if total > 0:
                pct = succeeded / total
                st.progress(pct)
                st.caption(f"{succeeded}/{total} complete ({pct:.0%})")

            elapsed_df = _safe_query(f"""
                SELECT
                    DATEDIFF('second',
                        MIN(SCHEDULED_TIME),
                        MAX(COALESCE(COMPLETED_TIME, CURRENT_TIMESTAMP()))
                    ) AS ELAPSED_SEC,
                    CASE WHEN COUNT_IF(COMPLETED_TIME IS NULL) > 0 THEN TRUE ELSE FALSE END AS STILL_RUNNING
                FROM TABLE({DATABASE}.INFORMATION_SCHEMA.TASK_HISTORY(
                    RESULT_LIMIT => 200
                ))
                WHERE NAME LIKE '{latest_dag}$%'
            """)
            if len(elapsed_df) > 0:
                elapsed = _safe_int(elapsed_df.iloc[0]["ELAPSED_SEC"])
                still_running = bool(elapsed_df.iloc[0]["STILL_RUNNING"])
                if still_running:
                    st.caption(
                        f"Elapsed: {elapsed:.0f}s (in progress, "
                        f"{succeeded}/{total} done)")
                else:
                    st.caption(f"Elapsed: {elapsed:.0f}s (all chunks done)")

            display_df = child_df.copy()
            if "DURATION_SEC" in display_df.columns:
                display_df["DURATION"] = display_df["DURATION_SEC"].apply(
                    lambda s: f"{s:.0f}s" if pd.notna(s) else "")

            with st.expander("Chunk detail", expanded=False):
                st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No child tasks found yet.")
    else:
        st.info("No task DAG runs found. Tasks phase hasn't started.")

    # ── SKU Progress (stage directory) ─────────────────────────────────
    st.subheader("SKU Progress (Stage)")
    try:
        session.sql(f"ALTER STAGE {STAGE_FQN} REFRESH").collect()
    except Exception:
        pass

    sku_df = _safe_query(f"""
        SELECT SPLIT_PART(RELATIVE_PATH, '/', 2) AS RUN_ID,
               COUNT(*)                           AS N_MODELS
        FROM DIRECTORY(@{STAGE_FQN})
        WHERE RELATIVE_PATH LIKE 'models/tasks\\_%/%.rds' ESCAPE '\\\\'
        GROUP BY RUN_ID
        ORDER BY MAX(LAST_MODIFIED) DESC
        LIMIT 5
    """)

    if len(sku_df) > 0:
        latest_run = str(sku_df.iloc[0]["RUN_ID"])
        latest_n = _safe_int(sku_df.iloc[0]["N_MODELS"])

        expected_df = _safe_query(f"""
            SELECT COUNT(DISTINCT UNIT_ID) AS N
            FROM {DATABASE}.SOURCE_DATA.SERIES_EVENTS
        """)
        expected = _safe_int(
            expected_df.iloc[0]["N"]) if len(expected_df) > 0 else 0

        cols = st.columns(3)
        cols[0].metric("Run ID", latest_run)
        cols[1].metric("Models Written", latest_n)
        cols[2].metric("Total SKUs", expected if expected > 0 else "?")

        if expected > 0:
            pct = min(latest_n / expected, 1.0)
            st.progress(pct)
            st.caption(f"{latest_n}/{expected} model files on stage "
                       f"({pct:.0%})")
        else:
            st.caption(f"{latest_n} model files on stage for `{latest_run}`")

        with st.expander("All runs on stage", expanded=False):
            st.dataframe(sku_df, use_container_width=True)
    else:
        st.caption("No model files found on stage yet.")

# ━━ Tab 3: Queue Phase ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_queue:
    st.header("Queue Phase (Dynamic Feeder)")

    # Scope to the most recent JOB_ID so counts aren't cumulative across runs
    job_id_df = _safe_query(f"""
        SELECT JOB_ID, COUNT(*) AS N
        FROM {QUEUE_FQN}
        GROUP BY JOB_ID
        ORDER BY MIN(CLAIMED_AT) DESC NULLS LAST
        LIMIT 10
    """)
    if len(job_id_df) > 0:
        job_ids = job_id_df["JOB_ID"].tolist()
        selected_job = st.sidebar.selectbox(
            "Queue Run (JOB_ID)", job_ids, index=0)
    else:
        selected_job = None

    if selected_job:
        job_filter = f"JOB_ID = '{selected_job}'"
        st.caption(f"Showing run: `{selected_job}`")
    else:
        job_filter = "1=1"

    summary_df = _safe_query(f"""
        SELECT STATUS, COUNT(*) AS N
        FROM {QUEUE_FQN}
        WHERE {job_filter}
        GROUP BY STATUS
        ORDER BY STATUS
    """)

    if len(summary_df) > 0:
        done = _safe_int(summary_df.loc[summary_df["STATUS"] == "DONE", "N"].sum())
        running = _safe_int(
            summary_df.loc[summary_df["STATUS"] == "RUNNING", "N"].sum())
        pending = _safe_int(
            summary_df.loc[summary_df["STATUS"] == "PENDING", "N"].sum())
        failed = _safe_int(
            summary_df.loc[summary_df["STATUS"] == "FAILED", "N"].sum())
        total = done + running + pending + failed

        cols = st.columns(5)
        cols[0].metric("Total Chunks", total)
        cols[1].metric("Done", done)
        cols[2].metric("Running", running)
        cols[3].metric("Pending", pending)
        cols[4].metric("Failed", failed)

        if total > 0:
            pct = done / total
            st.progress(pct)
            st.caption(f"{done}/{total} complete ({pct:.0%})")

        elapsed_df = _safe_query(f"""
            SELECT
                DATEDIFF('second', MIN(CLAIMED_AT), CURRENT_TIMESTAMP()) AS ELAPSED_NOW_SEC,
                DATEDIFF('second', MIN(CLAIMED_AT), MAX(COMPLETED_AT)) AS ELAPSED_DONE_SEC,
                COUNT_IF(STATUS IN ('RUNNING', 'PENDING')) AS STILL_ACTIVE
            FROM {QUEUE_FQN}
            WHERE {job_filter}
              AND CLAIMED_AT IS NOT NULL
        """)
        if len(elapsed_df) > 0 and pd.notna(elapsed_df.iloc[0]["ELAPSED_NOW_SEC"]):
            still_active = _safe_int(elapsed_df.iloc[0]["STILL_ACTIVE"])
            if still_active == 0 and done > 0:
                elapsed = _safe_int(elapsed_df.iloc[0]["ELAPSED_DONE_SEC"])
                st.caption(
                    f"Elapsed: {elapsed}s | "
                    f"Throughput: {done / max(elapsed, 1) * 60:.0f} chunks/min"
                )
            else:
                elapsed = _safe_int(elapsed_df.iloc[0]["ELAPSED_NOW_SEC"])
                st.caption(
                    f"Elapsed: {elapsed}s (in progress, "
                    f"{done}/{total} done, {running} running)"
                )

        chunk_df = _safe_query(f"""
            SELECT CHUNK_ID, STATUS, WORKER_ID,
                   CLAIMED_AT, COMPLETED_AT,
                   DATEDIFF('second', CLAIMED_AT, COMPLETED_AT) AS PROCESS_SEC
            FROM {QUEUE_FQN}
            WHERE {job_filter}
            ORDER BY CHUNK_ID
        """)

        if len(chunk_df) > 0:
            with st.expander("Per-chunk detail", expanded=False):
                st.dataframe(chunk_df, use_container_width=True)

            done_chunks = chunk_df.dropna(subset=["COMPLETED_AT"]).copy()
            if len(done_chunks) > 0:
                done_chunks["COMPLETED_AT"] = pd.to_datetime(
                    done_chunks["COMPLETED_AT"]).apply(_naive_ts)
                done_chunks = done_chunks.sort_values("COMPLETED_AT")
                done_chunks["CUMULATIVE"] = range(1, len(done_chunks) + 1)
                done_chunks = done_chunks.set_index("COMPLETED_AT")

                st.subheader("Cumulative Chunks Completed")
                st.line_chart(done_chunks[["CUMULATIVE"]])

            if "WORKER_ID" in chunk_df.columns:
                worker_df = chunk_df.dropna(subset=["WORKER_ID"])
                if len(worker_df) > 0:
                    worker_summary = (
                        worker_df.groupby("WORKER_ID")
                        .agg(CHUNKS=("CHUNK_ID", "count"),
                             AVG_SEC=("PROCESS_SEC", "mean"))
                        .reset_index()
                        .sort_values("CHUNKS", ascending=False)
                    )
                    st.subheader("Per-Worker Summary")
                    st.dataframe(worker_summary, use_container_width=True)

        if failed > 0:
            err_df = _safe_query(f"""
                SELECT CHUNK_ID, ERROR_MSG
                FROM {QUEUE_FQN}
                WHERE STATUS = 'FAILED' AND {job_filter}
                LIMIT 10
            """)
            if len(err_df) > 0:
                with st.expander("Failed chunk errors", expanded=True):
                    st.dataframe(err_df, use_container_width=True)
    else:
        st.info("No queue rows found. Queue phase hasn't started (or table missing).")

    # ── Queue SKU Progress (stage directory) ───────────────────────────
    st.subheader("SKU Progress (Stage)")
    queue_sku_df = _safe_query(f"""
        SELECT SPLIT_PART(RELATIVE_PATH, '/', 2) AS RUN_ID,
               COUNT(*)                           AS N_MODELS
        FROM DIRECTORY(@{STAGE_FQN})
        WHERE RELATIVE_PATH LIKE 'models/queue\\_%/%.rds' ESCAPE '\\\\'
        GROUP BY RUN_ID
        ORDER BY MAX(LAST_MODIFIED) DESC
        LIMIT 5
    """)
    if len(queue_sku_df) > 0:
        q_run = str(queue_sku_df.iloc[0]["RUN_ID"])
        q_n = _safe_int(queue_sku_df.iloc[0]["N_MODELS"])
        cols = st.columns(2)
        cols[0].metric("Run ID", q_run)
        cols[1].metric("Models Written", q_n)
        st.caption(f"{q_n} model files on stage for `{q_run}`")
        with st.expander("All queue runs on stage", expanded=False):
            st.dataframe(queue_sku_df, use_container_width=True)
    else:
        st.caption("No queue model files found on stage yet.")

if auto_refresh:
    time.sleep(5)
    _rerun()
