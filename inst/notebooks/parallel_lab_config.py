"""
Single source of truth for parallel-lab runtime identifiers.

Reads the `parallel_lab` (and optional `context`) section from
`snowflaker_parallel_spcs_config.yaml` next to this file. Used by:

- workspace_parallel_spcs_setup.ipynb
- workspace_parallel_spcs_demo.ipynb
- workspace_parallel_spcs_monitor.ipynb
- streamlit_parallel_demo_monitor.py

Snowflake internal benchmarks under internal/doSnowflake/tests/ still use
account-specific constants in Python — they are not wired to this file.
Public/generic demos should use only this YAML.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

DEFAULT_YAML = "snowflaker_parallel_spcs_config.yaml"

_ENV_PREFIX = "PARALLEL_LAB_"


def _notebook_dir() -> Path:
    """Directory containing sfnb_setup.py and *_config.yaml (inst/notebooks)."""
    try:
        import sfnb_setup  # type: ignore

        return Path(sfnb_setup.__file__).resolve().parent
    except Exception:
        return Path(__file__).resolve().parent


def config_yaml_path(explicit: str | None = None) -> Path:
    if explicit:
        p = Path(explicit)
        return p if p.is_absolute() else (_notebook_dir() / p)
    return _notebook_dir() / DEFAULT_YAML


def load_parallel_lab_dict(config_path: str | None = None) -> dict[str, Any]:
    """Load merged `parallel_lab` settings (with defaults). Does not set os.environ."""
    import yaml

    path = config_yaml_path(config_path)
    with open(path, encoding="utf-8") as f:
        full: dict[str, Any] = yaml.safe_load(f) or {}

    ctx = full.get("context") or {}
    if not isinstance(ctx, dict):
        ctx = {}

    lab_in = full.get("parallel_lab") or {}
    if not isinstance(lab_in, dict):
        lab_in = {}

    defaults: dict[str, Any] = {
        "database": "SFLAB_EP_DEMO",
        "schemas": {
            "source_data": "SOURCE_DATA",
            "config": "CONFIG",
            "models": "MODELS",
        },
        "warehouse": "",
        "compute_pool": "",
        "image_uri": "",
        "dosnowflake_stage_name": "DOSNOWFLAKE_STAGE",
        "queue_table": "DOSNOWFLAKE_QUEUE",
        # When false, setup notebook skips SERIES_EVENTS (use existing SOURCE_DATA, e.g. SALES_DATA).
        "create_synthetic_series_table": True,
        # Demo notebook: forecast foreach scale (see test_dosnowflake_tasks_benchmark.py).
        "demo_forecast_n_skus": 2000,
        "demo_tasks_chunks_per_job": 10,
        "demo_queue_n_workers": 10,
        "demo_queue_chunks_per_job": 10,
        # When false, skip customer-name substring guard in validate (internal diagnostics).
        "clean_room": True,
    }

    lab = {**defaults, **{k: v for k, v in lab_in.items() if k != "schemas"}}
    sch = dict(defaults["schemas"])
    if isinstance(lab_in.get("schemas"), dict):
        sch.update({k: v for k, v in lab_in["schemas"].items() if v})
    lab["schemas"] = sch

    # Warehouse: parallel_lab overrides context
    if not (lab.get("warehouse") or "").strip():
        w = ctx.get("warehouse")
        if w:
            lab["warehouse"] = str(w)

    lab["_config_path"] = str(path)
    lab["_context"] = ctx
    return lab


def fq_schema_table(cfg: dict[str, Any], schema_role: str, table: str) -> str:
    """Fully qualified TABLE (DATABASE.SCHEMA.TABLE). schema_role: source_data | config | models."""
    db = cfg["database"]
    sch = cfg["schemas"][schema_role]
    return f"{db}.{sch}.{table}"


def stage_at(cfg: dict[str, Any]) -> str:
    """Stage for @DATABASE.SCHEMA.STAGE_NAME (doSnowflake uploads)."""
    db = cfg["database"]
    sch_st = cfg["schemas"]["source_data"]
    name = cfg.get("dosnowflake_stage_name") or "DOSNOWFLAKE_STAGE"
    return f"@{db}.{sch_st}.{name}"


def queue_fqn(cfg: dict[str, Any]) -> str:
    db = cfg["database"]
    sch = cfg["schemas"]["config"]
    qt = cfg.get("queue_table") or "DOSNOWFLAKE_QUEUE"
    return f"{db}.{sch}.{qt}"


def apply_parallel_lab_environment(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Populate os.environ so %%R cells can use Sys.getenv().
    Call after setup_notebook() if you rely on session defaults, or before — either is fine.
    """
    cfg = cfg or load_parallel_lab_dict()
    sch = cfg["schemas"]

    os.environ[f"{_ENV_PREFIX}DATABASE"] = str(cfg["database"])
    os.environ[f"{_ENV_PREFIX}SCHEMA_SOURCE_DATA"] = str(sch["source_data"])
    os.environ[f"{_ENV_PREFIX}SCHEMA_CONFIG"] = str(sch["config"])
    os.environ[f"{_ENV_PREFIX}SCHEMA_MODELS"] = str(sch["models"])
    os.environ[f"{_ENV_PREFIX}WAREHOUSE"] = str(cfg.get("warehouse") or "")
    os.environ[f"{_ENV_PREFIX}COMPUTE_POOL"] = str(cfg.get("compute_pool") or "")
    os.environ[f"{_ENV_PREFIX}IMAGE_URI"] = str(cfg.get("image_uri") or "")
    os.environ[f"{_ENV_PREFIX}STAGE_AT"] = stage_at(cfg)
    os.environ[f"{_ENV_PREFIX}QUEUE_FQN"] = queue_fqn(cfg)
    os.environ[f"{_ENV_PREFIX}CONFIG_PATH"] = str(cfg.get("_config_path", ""))
    os.environ[f"{_ENV_PREFIX}CREATE_SYNTHETIC_SERIES"] = (
        "1" if cfg.get("create_synthetic_series_table", True) else "0"
    )
    os.environ[f"{_ENV_PREFIX}DEMO_FORECAST_N_SKUS"] = str(
        int(cfg.get("demo_forecast_n_skus", 2000))
    )
    os.environ[f"{_ENV_PREFIX}DEMO_TASKS_CHUNKS_PER_JOB"] = str(
        int(cfg.get("demo_tasks_chunks_per_job", 10))
    )
    os.environ[f"{_ENV_PREFIX}DEMO_QUEUE_N_WORKERS"] = str(
        int(cfg.get("demo_queue_n_workers", 10))
    )
    os.environ[f"{_ENV_PREFIX}DEMO_QUEUE_CHUNKS_PER_JOB"] = str(
        int(cfg.get("demo_queue_chunks_per_job", 10))
    )
    return cfg


def ensure_notebook_path_on_sys_path() -> None:
    """Import sfnb_setup from inst/notebooks when the kernel cwd is elsewhere."""
    nd = _notebook_dir()
    s = str(nd)
    if s not in sys.path:
        sys.path.insert(0, s)
