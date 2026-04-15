"""
Embarrassingly parallel lab: live monitoring (Streamlit in Snowflake).

FQNs are built from the same `parallel_lab` section as the notebooks
(`snowflaker_parallel_spcs_config.yaml` via `parallel_lab_config.py`).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from snowflake.snowpark import Session

# Ensure inst/notebooks is on path when SiS cwd differs
_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import parallel_lab_config as plc  # noqa: E402

try:
    from snowflake.snowpark.context import get_active_session

    session = get_active_session()
except Exception:
    session = Session.builder.config("connection_name", "default").create()

LAB = plc.load_parallel_lab_dict()
JM = plc.fq_schema_table(LAB, "config", "JOB_MANIFEST")
TR = plc.fq_schema_table(LAB, "models", "TRAINING_RESULTS")
QQ = plc.queue_fqn(LAB)

_cfg_path = LAB.get("_config_path") or ""
_cfg_name = Path(_cfg_path).name if _cfg_path else plc.DEFAULT_YAML

st.set_page_config(page_title="Parallel lab monitor", layout="wide")
st.title("Embarrassingly parallel lab — live status")
st.caption(f"Database `{LAB['database']}` — parallel_lab from `{_cfg_name}`")

auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)
st.sidebar.button("Refresh Now")

try:
    jobs = session.sql(
        f"""
        SELECT DISTINCT JOB_ID, JOB_TYPE AS SOURCE,
               MIN(STARTED_AT) AS STARTED, COUNT(*) AS N_ITEMS
        FROM {JM}
        GROUP BY JOB_ID, JOB_TYPE
        ORDER BY STARTED DESC NULLS LAST
        LIMIT 30
        """
    ).to_pandas()
except Exception as e:
    st.error(f"JOB_MANIFEST query failed (run setup notebook?): {e}")
    st.stop()

job_labels = {"All jobs": "All jobs"}
if len(jobs) > 0:
    for _, row in jobs.iterrows():
        ts = pd.to_datetime(row["STARTED"]).strftime("%H:%M") if pd.notna(row["STARTED"]) else "?"
        label = f"{row['SOURCE']} | {int(row['N_ITEMS'])} rows | {ts} | {str(row['JOB_ID'])[:10]}..."
        job_labels[row["JOB_ID"]] = label

selected = st.sidebar.selectbox(
    "Job filter",
    list(job_labels.keys()),
    format_func=lambda x: job_labels.get(x, x),
)
job_filter = "" if selected == "All jobs" else f"AND JOB_ID = '{selected}'"

tab_train, tab_metrics = st.tabs(["Manifest", "Training metrics"])

with tab_train:
    st.subheader("Manifest by status")
    try:
        mdf = session.sql(
            f"""
            SELECT JOB_ID, JOB_TYPE, STATUS, COUNT(*) AS N,
                   MIN(STARTED_AT) AS FIRST_START,
                   MAX(COMPLETED_AT) AS LAST_COMPLETE
            FROM {JM}
            WHERE 1=1 {job_filter}
            GROUP BY JOB_ID, JOB_TYPE, STATUS
            ORDER BY JOB_ID, STATUS
            """
        ).to_pandas()
        if len(mdf) > 0:
            st.dataframe(mdf, use_container_width=True)
        else:
            st.info("No manifest rows yet.")
    except Exception as e:
        st.warning(str(e))

    st.subheader("doSnowflake queue (optional)")
    try:
        qdf = session.sql(
            f"""
            SELECT JOB_ID, STATUS, COUNT(*) AS N
            FROM {QQ}
            WHERE 1=1 {job_filter}
            GROUP BY JOB_ID, STATUS
            ORDER BY JOB_ID, STATUS
            """
        ).to_pandas()
        if len(qdf) > 0:
            st.dataframe(qdf, use_container_width=True)
        else:
            st.caption("No queue rows (queue mode not used or table missing).")
    except Exception:
        st.caption("DOSNOWFLAKE_QUEUE not available.")

with tab_metrics:
    st.subheader("TRAINING_RESULTS (latest)")
    try:
        rdf = session.sql(
            f"""
            SELECT JOB_ID, UNIT_ID, WORKER_INDEX, RMSE, MAE, AIC,
                   TRAINING_SECS, COMPLETED_AT
            FROM {TR}
            WHERE 1=1 {job_filter}
            ORDER BY COMPLETED_AT DESC
            LIMIT 500
            """
        ).to_pandas()
        if len(rdf) > 0:
            st.dataframe(rdf, use_container_width=True)
        else:
            st.info("No training metrics yet.")
    except Exception as e:
        st.warning(str(e))

if auto_refresh:
    time.sleep(5)
    st.rerun()
