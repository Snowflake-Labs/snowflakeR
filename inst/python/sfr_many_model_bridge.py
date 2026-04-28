"""
Many-Model Registry Bridge for R Models
========================================

Python backend for snowflakeR many-model aggregator pattern.

Registers a SINGLE CustomModel that dispatches inference to per-partition
.rds files stored on an internal stage. Supports arbitrary partition keys
(SKU, SKU+STORE, marketing segments, etc.).

Architecture:
    R user code
        -> snowflakeR::R/many_model.R  (user-facing R functions)
        -> reticulate bridge
        -> sfr_many_model_bridge.py  (this file)
        -> snowflake.ml.registry + snowflake.ml.model.custom_model

Depends on: sfr_registry_bridge.py for session helpers.
"""

import json
import os
import tempfile
import textwrap
from typing import Dict, List, Optional

import pandas as pd


def _build_aggregator_class(partition_columns: List[str],
                            r_packages: List[str],
                            horizon: int = 12,
                            database: str = "DEMO_DB",
                            schema: str = "MODELS"):
    """Build a CustomModel subclass for many-model R forecast aggregator.

    Uses @partitioned_api for run_batch distribution and
    @inference_api for SPCS service deployment.
    """
    from snowflake.ml.model import custom_model

    pcols = list(partition_columns)
    pkgs = list(r_packages)
    h = horizon
    _db = database
    _sc = schema

    class RManyModelAggregator(custom_model.CustomModel):

        def __init__(self, context: custom_model.ModelContext):
            super().__init__(context)
            self._r_initialized = False
            self._index = None
            self._sf_conn = None
            self._models_dir = "/tmp/rds_models"

        def _init_r(self):
            if self._r_initialized:
                return
            import rpy2.robjects as ro
            for pkg in pkgs:
                ro.r(f"suppressMessages(library({pkg}))")
            self._r_initialized = True

        def _get_index(self):
            if self._index is None:
                with open(self.context.path("model_index")) as f:
                    self._index = json.load(f)
            return self._index

        def _get_sf_connection(self):
            if self._sf_conn is not None:
                return self._sf_conn
            import snowflake.connector
            token_path = "/snowflake/session/token"
            try:
                with open(token_path) as f:
                    token = f.read().strip()
                self._sf_conn = snowflake.connector.connect(
                    host=os.environ.get("SNOWFLAKE_HOST", ""),
                    account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                    authenticator="oauth",
                    token=token,
                    database=_db,
                    schema=_sc,
                )
            except FileNotFoundError:
                self._sf_conn = snowflake.connector.connect(
                    account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                    user=os.environ.get("SNOWFLAKE_USER", ""),
                    password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
                    database=_db,
                    schema=_sc,
                )
            return self._sf_conn

        def _ensure_rds_local(self, partition_key, stage_path):
            local_path = os.path.join(self._models_dir, f"{partition_key}.rds")
            if os.path.exists(local_path):
                return local_path
            os.makedirs(self._models_dir, exist_ok=True)
            conn = self._get_sf_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    f"GET '{stage_path}' 'file://{self._models_dir}/' PARALLEL=4"
                )
            finally:
                cur.close()
            downloaded = os.path.join(self._models_dir, os.path.basename(stage_path))
            if downloaded != local_path and os.path.exists(downloaded):
                os.rename(downloaded, local_path)
            return local_path

        def _do_predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            from rpy2.robjects.conversion import localconverter

            self._init_r()
            index = self._get_index()

            key_parts = [str(input_df[c].iloc[0]) for c in pcols]
            partition_key = "__".join(key_parts)

            if partition_key not in index:
                return pd.DataFrame({
                    "PARTITION_KEY": [partition_key],
                    "PERIOD": [0],
                    "FORECAST": [float("nan")],
                    "LOWER_80": [float("nan")],
                    "UPPER_80": [float("nan")],
                    "LOWER_95": [float("nan")],
                    "UPPER_95": [float("nan")],
                    "STATUS": ["NO_MODEL"],
                })

            stage_path = index[partition_key]
            try:
                local_rds = self._ensure_rds_local(partition_key, stage_path)
                combined = ro.default_converter + pandas2ri.converter
                with localconverter(combined):
                    ro.r(f'm <- readRDS("{local_rds}")')
                    ro.r(f"fc <- forecast::forecast(m, h = {h})")
                    result = ro.r(textwrap.dedent(f"""\
                        data.frame(
                            PARTITION_KEY = rep("{partition_key}", {h}),
                            PERIOD        = seq_len({h}),
                            FORECAST      = as.numeric(fc$mean),
                            LOWER_80      = as.numeric(fc$lower[, 1]),
                            UPPER_80      = as.numeric(fc$upper[, 1]),
                            LOWER_95      = as.numeric(fc$lower[, 2]),
                            UPPER_95      = as.numeric(fc$upper[, 2]),
                            STATUS        = rep("OK", {h})
                        )
                    """))
                    result_df = ro.conversion.rpy2py(result)
                ro.r("rm(m, fc)")
                return result_df
            except Exception as e:
                return pd.DataFrame({
                    "PARTITION_KEY": [partition_key],
                    "PERIOD": [0],
                    "FORECAST": [float("nan")],
                    "LOWER_80": [float("nan")],
                    "UPPER_80": [float("nan")],
                    "LOWER_95": [float("nan")],
                    "UPPER_95": [float("nan")],
                    "STATUS": [str(e)[:200]],
                })

        @custom_model.inference_api
        def predict_single(self, input_df: pd.DataFrame) -> pd.DataFrame:
            return self._do_predict(input_df)

        @custom_model.partitioned_api
        def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
            return self._do_predict(input_df)

    return RManyModelAggregator


def registry_log_many_model(session,
                            model_name: str,
                            version_name: str,
                            model_index: Dict[str, str],
                            partition_columns: Optional[List[str]] = None,
                            r_packages: Optional[List[str]] = None,
                            horizon: int = 12,
                            database_name: Optional[str] = None,
                            schema_name: Optional[str] = None,
                            comment: Optional[str] = None):
    """Register a many-model R forecast aggregator in Model Registry.

    Parameters
    ----------
    session : Snowpark Session
    model_name : str
    version_name : str
    model_index : dict
        Mapping of partition_key -> stage_path for each .rds model file.
    partition_columns : list of str, optional
        Column names that form the partition key (default: ["SKU"]).
    r_packages : list of str, optional
        R packages to load at predict time (default: ["forecast"]).
    horizon : int
        Forecast horizon (default: 12).
    database_name, schema_name : str, optional
    comment : str, optional

    Returns
    -------
    dict with keys: model_name, version_name, n_partitions, registration_time_s
    """
    from snowflake.ml.registry import Registry
    from snowflake.ml.model import custom_model, model_signature

    if partition_columns is None:
        partition_columns = ["SKU"]
    if r_packages is None:
        r_packages = ["forecast"]

    AggregatorClass = _build_aggregator_class(
        partition_columns=partition_columns,
        r_packages=r_packages,
        horizon=horizon,
        database=database_name or "DEMO_DB",
        schema=schema_name or "MODELS",
    )

    index_dir = tempfile.mkdtemp(prefix="sfr_mm_")
    index_path = os.path.join(index_dir, "model_index.json")
    with open(index_path, "w") as f:
        json.dump(model_index, f)

    ctx = custom_model.ModelContext(artifacts={"model_index": index_path})
    model_instance = AggregatorClass(ctx)

    input_features = []
    for col in partition_columns:
        input_features.append(
            model_signature.FeatureSpec(name=col, dtype=model_signature.DataType.STRING)
        )
    input_features.append(
        model_signature.FeatureSpec(name="SALES", dtype=model_signature.DataType.DOUBLE)
    )
    output_features = [
        model_signature.FeatureSpec(name="PARTITION_KEY", dtype=model_signature.DataType.STRING),
        model_signature.FeatureSpec(name="PERIOD", dtype=model_signature.DataType.INT64),
        model_signature.FeatureSpec(name="FORECAST", dtype=model_signature.DataType.DOUBLE),
        model_signature.FeatureSpec(name="LOWER_80", dtype=model_signature.DataType.DOUBLE),
        model_signature.FeatureSpec(name="UPPER_80", dtype=model_signature.DataType.DOUBLE),
        model_signature.FeatureSpec(name="LOWER_95", dtype=model_signature.DataType.DOUBLE),
        model_signature.FeatureSpec(name="UPPER_95", dtype=model_signature.DataType.DOUBLE),
        model_signature.FeatureSpec(name="STATUS", dtype=model_signature.DataType.STRING),
    ]
    sig = model_signature.ModelSignature(inputs=input_features, outputs=output_features)
    signatures = {"predict": sig, "predict_single": sig}

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name
    reg = Registry(**reg_kwargs)

    if comment is None:
        comment = (
            f"Many-model R forecast aggregator | "
            f"{len(model_index)} partitions | cols={partition_columns}"
        )

    import time
    t0 = time.time()
    mv = reg.log_model(
        model_instance,
        model_name=model_name,
        version_name=version_name,
        conda_dependencies=[
            "r-base>=4.1",
            "rpy2>=3.5",
            "r-forecast",
            "numpy<2.0",
        ],
        pip_requirements=["snowflake-connector-python"],
        target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
        options={
            "method_options": {
                "predict": {"function_type": "TABLE_FUNCTION"},
                "predict_single": {"function_type": "FUNCTION"},
            }
        },
        signatures=signatures,
        comment=comment,
    )
    reg_time = time.time() - t0

    return {
        "model_name": mv.model_name,
        "version_name": mv.version_name,
        "n_partitions": len(model_index),
        "registration_time_s": round(reg_time, 1),
    }


def build_model_index_from_view(session,
                                view_fqn: str = "DEMO_DB.MODELS.MODEL_INDEX",
                                run_id: Optional[str] = None,
                                stage_prefix: str = "@DEMO_DB.MODELS.MODELS_STAGE/"):
    """Build model index dict from MODEL_INDEX view.

    Returns dict: {partition_key: stage_path}
    """
    where = f" WHERE RUN_ID = '{run_id}'" if run_id else ""
    rows = session.sql(
        f"SELECT PARTITION_KEY, RELATIVE_PATH FROM {view_fqn}{where}"
    ).collect()

    return {
        row["PARTITION_KEY"]: f"{stage_prefix}{row['RELATIVE_PATH']}"
        for row in rows
    }
