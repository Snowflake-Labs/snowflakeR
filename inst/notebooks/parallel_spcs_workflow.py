"""
Core workflow helpers for the parallel SPCS demo.

This module is intentionally Notebook-agnostic so we can validate behavior
locally before running inside Snowflake Workspace notebooks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import parallel_lab_config as plc


def _qident(value: str) -> str:
    """Conservative SQL identifier validation."""
    allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    if not value or any(ch not in allowed for ch in value.upper()):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def _qn(db: str, schema: str, obj: str) -> str:
    return f"{_qident(db)}.{_qident(schema)}.{_qident(obj)}"


@dataclass(frozen=True)
class ParallelLabNames:
    database: str
    source_schema: str
    config_schema: str
    models_schema: str
    stage_name: str
    queue_table: str
    warehouse: str
    compute_pool: str
    image_uri: str
    # When False, skip MR_PRICE substring guard (internal diagnostics only).
    clean_room: bool = True

    @classmethod
    def from_cfg(cls, cfg: dict) -> "ParallelLabNames":
        sch = cfg["schemas"]
        cr = cfg.get("clean_room", True)
        if not isinstance(cr, bool):
            cr = bool(cr)
        return cls(
            database=str(cfg["database"]),
            source_schema=str(sch["source_data"]),
            config_schema=str(sch["config"]),
            models_schema=str(sch["models"]),
            stage_name=str(
                cfg.get("dosnowflake_stage_name") or "DOSNOWFLAKE_STAGE"
            ),
            queue_table=str(cfg.get("queue_table") or "DOSNOWFLAKE_QUEUE"),
            warehouse=str(cfg.get("warehouse") or ""),
            compute_pool=str(cfg.get("compute_pool") or ""),
            image_uri=str(cfg.get("image_uri") or ""),
            clean_room=cr,
        )

    def contains_forbidden_refs(
        self, forbidden: Iterable[str] = ("MR_PRICE",)
    ) -> list[str]:
        all_values = [
            self.database,
            self.source_schema,
            self.config_schema,
            self.models_schema,
            self.stage_name,
            self.queue_table,
            self.warehouse,
            self.compute_pool,
            self.image_uri,
        ]
        hits: list[str] = []
        upper_values = [v.upper() for v in all_values if v]
        for needle in forbidden:
            n = needle.upper()
            if any(n in v for v in upper_values):
                hits.append(needle)
        return hits

    def validate_clean_room(self) -> None:
        if self.clean_room:
            hits = self.contains_forbidden_refs()
            if hits:
                joined = ", ".join(sorted(set(hits)))
                raise ValueError(
                    f"Config still contains forbidden identifiers: {joined}"
                )
        if not self.image_uri:
            raise ValueError(
                "parallel_lab.image_uri is required for tasks/queue demos"
            )
        if not self.compute_pool:
            raise ValueError(
                "parallel_lab.compute_pool is required for tasks/queue demos"
            )


def sql_bootstrap(names: ParallelLabNames) -> list[str]:
    """Generate clean-slate SQL for DB, schemas, stage, and queue table."""
    db = _qident(names.database)
    src = _qident(names.source_schema)
    cfg = _qident(names.config_schema)
    mdl = _qident(names.models_schema)
    stage = _qident(names.stage_name)
    queue = _qident(names.queue_table)

    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"CREATE SCHEMA IF NOT EXISTS {db}.{src}",
        f"CREATE SCHEMA IF NOT EXISTS {db}.{cfg}",
        f"CREATE SCHEMA IF NOT EXISTS {db}.{mdl}",
        f"CREATE STAGE IF NOT EXISTS {db}.{src}.{stage}",
        (
            f"CREATE TABLE IF NOT EXISTS {db}.{cfg}.{queue} ("
            "TASK_ID VARCHAR, STATUS VARCHAR, PAYLOAD VARIANT, "
            "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
        ),
    ]


def sql_seed_series_events(
    names: ParallelLabNames,
    n_units: int = 120,
    n_days: int = 365,
    start_date: str = "2023-01-01",
) -> str:
    tbl = _qn(names.database, names.source_schema, "SERIES_EVENTS")
    if n_units < 1 or n_days < 1:
        raise ValueError("n_units and n_days must be >= 1")
    return (
        f"CREATE OR REPLACE TABLE {tbl} AS "
        f"WITH units AS ("
        f"  SELECT LPAD(TO_VARCHAR(SEQ4()), 6, '0') AS UNIT_ID "
        f"  FROM TABLE(GENERATOR(ROWCOUNT => {int(n_units)})) "
        f"), days AS ("
        f"  SELECT SEQ4() AS d "
        f"FROM TABLE(GENERATOR(ROWCOUNT => {int(n_days)})) "
        f") "
        "SELECT u.UNIT_ID, "
        f"       DATEADD('day', d.d, DATE '{start_date}') AS OBS_DATE, "
        "       10 + MOD(ABS(HASH(u.UNIT_ID, d.d)), 80) * 0.05 "
        "+ SIN(d.d / 25.0) AS Y "
        "FROM units u CROSS JOIN days d"
    )


def load_names_from_yaml(config_path: str | None = None) -> ParallelLabNames:
    return ParallelLabNames.from_cfg(plc.load_parallel_lab_dict(config_path))
