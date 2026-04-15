"""
Generic R Executor via Model Registry (Strategy C)
===================================================

Registers a CustomModel in Snowflake Model Registry that executes
arbitrary R code per partition. The R script is stored as a model
artifact and sourced at runtime via rpy2.

Supports two usage modes:
  1. **Result mode** -- R function returns a data.frame that becomes
     TABLE_FUNCTION output rows.
  2. **Diagnostic mode** -- R function writes results elsewhere (stage,
     table) and returns only execution diagnostics (status, timing,
     error messages).

Architecture:
    R user code
        -> snowflakeR::sfr_log_executor()
        -> reticulate bridge
        -> sfr_r_executor_bridge.py  (this file)
        -> snowflake.ml.registry + snowflake.ml.model.custom_model
"""

import json
import os
import tempfile
import time
from typing import Any, Dict, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# CustomModel factory
# ---------------------------------------------------------------------------

def _build_executor_class(
    partition_columns: List[str],
    r_packages: List[str],
    r_entry_fn: str = "main",
    r_script_artifact: str = "r_script",
    output_columns: Optional[List[Dict[str, str]]] = None,
    diagnostic_mode: bool = False,
):
    """Build a CustomModel subclass that executes arbitrary R code.

    Parameters
    ----------
    partition_columns : list of str
        Column names used for PARTITION BY.
    r_packages : list of str
        R packages to load before sourcing the script.
    r_entry_fn : str
        Name of the R function to call per partition.
    r_script_artifact : str
        Artifact key for the R script file.
    output_columns : list of dict, optional
        Output schema as [{"name": "COL", "type": "STRING"}, ...].
        If None, uses default diagnostic schema.
    diagnostic_mode : bool
        If True, forces diagnostic-only output regardless of what the
        R function returns.
    """
    from snowflake.ml.model import custom_model

    pcols = list(partition_columns)
    pkgs = list(r_packages)
    entry_fn = r_entry_fn
    script_key = r_script_artifact
    diag_mode = diagnostic_mode
    out_col_names = [c["name"] for c in output_columns] if output_columns else []

    class RExecutor(custom_model.CustomModel):

        def __init__(self, context: custom_model.ModelContext):
            super().__init__(context)
            self._r_initialized = False
            self._sf_conn = None

        def _init_r(self):
            if self._r_initialized:
                return
            import rpy2.robjects as ro

            for pkg in pkgs:
                ro.r(f'suppressMessages(library({pkg}))')

            script_path = self.context.path(script_key)
            ro.r(f'source("{script_path}")')
            self._r_initialized = True

        def _get_sf_connection(self):
            """Snowflake connection for stage I/O within R code."""
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
                )
            except FileNotFoundError:
                self._sf_conn = snowflake.connector.connect(
                    account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                    user=os.environ.get("SNOWFLAKE_USER", ""),
                    password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
                )
            return self._sf_conn

        def _do_execute(self, input_df: pd.DataFrame) -> pd.DataFrame:
            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            from rpy2.robjects.conversion import localconverter

            if len(input_df) == 0:
                return pd.DataFrame(columns=out_col_names)

            self._init_r()

            key_parts = [str(input_df[c].iloc[0]) for c in pcols]
            partition_key = "__".join(key_parts)

            t0 = time.time()
            rows_in = len(input_df)

            try:
                combined = ro.default_converter + pandas2ri.converter
                with localconverter(combined):
                    ro.globalenv["input_data"] = input_df
                    ro.r(f'result <- {entry_fn}(input_data)')
                    result_r = ro.globalenv["result"]
                    result_df = ro.conversion.rpy2py(result_r)

                elapsed = round(time.time() - t0, 3)
                ro.r("rm(input_data, result)")

                if diag_mode or not isinstance(result_df, pd.DataFrame):
                    return self._make_diag(partition_key, "OK", elapsed, rows_in,
                                           rows_out=len(result_df) if isinstance(result_df, pd.DataFrame) else 0)

                result_df = self._to_plain_df(result_df)

                if "PARTITION_KEY" not in result_df.columns:
                    result_df["PARTITION_KEY"] = partition_key
                if "STATUS" not in result_df.columns:
                    result_df["STATUS"] = "OK"
                if "ELAPSED_SEC" not in result_df.columns:
                    result_df["ELAPSED_SEC"] = elapsed

                # Safety: deduplicate columns to prevent Ray size_bytes() crash
                if result_df.columns.duplicated().any():
                    result_df = result_df.loc[:, ~result_df.columns.duplicated()]
                return result_df

            except Exception as e:
                elapsed = round(time.time() - t0, 3)
                ro.r('if (exists("input_data")) rm(input_data)')
                ro.r('if (exists("result")) rm(result)')
                if out_col_names:
                    err_data = {}
                    for cn in out_col_names:
                        err_data[cn] = [""]
                    err_data["PARTITION_KEY"] = [partition_key]
                    err_data["STATUS"] = [f"ERROR: {str(e)[:500]}"]
                    err_data["ELAPSED_SEC"] = [elapsed]
                    return pd.DataFrame(err_data)
                return self._make_diag(partition_key, "ERROR", elapsed, rows_in,
                                       error_msg=str(e)[:2000])

        @staticmethod
        def _to_plain_df(df):
            """Rebuild DataFrame from rpy2 output using numpy-native dtypes.

            Snowflake's ray_inference_job.py calls .fillna() on model output,
            which with pandas 2.x + object-typed columns triggers downcasting
            into extension dtypes (Int64, string[python]) that break Ray's
            BlockAccessor.size_bytes(). We avoid this by:
            1. Converting all values to plain Python types
            2. Inferring and setting explicit numpy dtypes (str→str, num→float64)
            3. Replacing all NaN/None with concrete defaults
            """
            import numpy as np
            col_names = [str(c) for c in df.columns]
            new_df = pd.DataFrame()
            for i, col in enumerate(col_names):
                raw = df.iloc[:, i]
                vals = []
                has_numeric = False
                has_string = False
                for j in range(len(raw)):
                    v = raw.iloc[j]
                    if v is None:
                        vals.append(None)
                    elif isinstance(v, (int, float, np.integer, np.floating)):
                        has_numeric = True
                        vals.append(float(v) if hasattr(v, 'item') else float(v))
                    elif hasattr(v, 'item'):
                        has_numeric = True
                        vals.append(v.item())
                    else:
                        has_string = True
                        vals.append(str(v))

                if has_numeric and not has_string:
                    filled = [0.0 if v is None else float(v) for v in vals]
                    new_df[col] = np.array(filled, dtype=np.float64)
                else:
                    filled = ["" if v is None else str(v) for v in vals]
                    new_df[col] = np.array(filled, dtype=object)
            return new_df

        @staticmethod
        def _make_diag(partition_key, status, elapsed_sec, rows_in,
                       rows_out=0, error_msg="", result_json=""):
            return pd.DataFrame({
                "PARTITION_KEY": [str(partition_key)],
                "STATUS": [str(status)],
                "ELAPSED_SEC": [float(elapsed_sec)],
                "ROWS_IN": [int(rows_in)],
                "ROWS_OUT": [int(rows_out)],
                "ERROR_MSG": [str(error_msg) if error_msg else None],
                "RESULT_JSON": [str(result_json) if result_json else None],
            })

        @custom_model.partitioned_api
        def execute(self, input_df: pd.DataFrame) -> pd.DataFrame:
            return self._do_execute(input_df)

        @custom_model.inference_api
        def execute_single(self, input_df: pd.DataFrame) -> pd.DataFrame:
            """Process each row individually and concatenate results."""
            results = []
            for idx in range(len(input_df)):
                row_df = input_df.iloc[[idx]].reset_index(drop=True)
                result = self._do_execute(row_df)
                results.append(result)
            if results:
                combined = pd.concat(results, ignore_index=True)
                return self._to_plain_df(combined)
            return self._do_execute(input_df)

    return RExecutor


def _rebuild_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild a DataFrame from rpy2 conversion into a fresh pandas DataFrame
    using only Python-native lists and numpy-native dtypes.

    rpy2 can produce DataFrames with internal structures (MultiIndex,
    R-specific extension arrays, duplicate-looking columns) that cause
    Ray's BlockAccessor.size_bytes() to crash. This function extracts every
    column as a plain Python list and constructs a brand-new DataFrame.
    """
    col_names = [str(c) for c in df.columns]
    data = {}
    for i, col in enumerate(col_names):
        series = df.iloc[:, i]
        try:
            arr = series.to_numpy(dtype=object, na_value=None)
        except (TypeError, ValueError):
            arr = [series.iloc[j] for j in range(len(series))]
        py_list = []
        for v in arr:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                py_list.append(None)
            elif hasattr(v, 'item'):
                py_list.append(v.item())
            else:
                py_list.append(v)
        data[col] = py_list
    return pd.DataFrame(data)


def _diagnostic_row(
    partition_key: str,
    status: str,
    elapsed_sec: float,
    rows_in: int,
    rows_out: int = 0,
    error_msg: str = "",
    result_json: str = "",
) -> pd.DataFrame:
    """Build a single diagnostic row."""
    return pd.DataFrame({
        "PARTITION_KEY": [partition_key],
        "STATUS": [status],
        "ELAPSED_SEC": [elapsed_sec],
        "ROWS_IN": [rows_in],
        "ROWS_OUT": [rows_out],
        "ERROR_MSG": [error_msg if error_msg else None],
        "RESULT_JSON": [result_json if result_json else None],
    })


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def registry_log_executor(
    session,
    model_name: str,
    version_name: str,
    r_script_path: str,
    partition_columns: Optional[List[str]] = None,
    r_packages: Optional[List[str]] = None,
    entry_fn: str = "main",
    diagnostic_mode: bool = False,
    input_columns: Optional[List[Dict[str, str]]] = None,
    output_columns: Optional[List[Dict[str, str]]] = None,
    conda_dependencies: Optional[List[str]] = None,
    pip_requirements: Optional[List[str]] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    comment: Optional[str] = None,
) -> Dict[str, Any]:
    """Register a generic R executor model in Model Registry.

    Parameters
    ----------
    session : Snowpark Session
    model_name : str
    version_name : str
    r_script_path : str
        Local path to the R script. Must define the entry function.
    partition_columns : list of str, optional
        Column names for PARTITION BY (default: ["SKU"]).
    r_packages : list of str, optional
        R packages to load (default: []).
    entry_fn : str
        Name of the R function to call per partition.
    diagnostic_mode : bool
        If True, executor returns diagnostics only (not R results).
    input_columns : list of dict, optional
        Input schema as [{"name": "COL", "type": "STRING"}, ...].
        If None, uses partition columns as STRING inputs.
    output_columns : list of dict, optional
        Output schema. If None, uses default diagnostic schema.
    conda_dependencies : list of str, optional
        Conda packages for the model container.
    pip_requirements : list of str, optional
        Pip packages for the model container.
    database_name, schema_name : str, optional
    comment : str, optional

    Returns
    -------
    dict with: model_name, version_name, registration_time_s
    """
    from snowflake.ml.registry import Registry
    from snowflake.ml.model import custom_model, model_signature

    if partition_columns is None:
        partition_columns = ["SKU"]
    if r_packages is None:
        r_packages = []
    if conda_dependencies is None:
        conda_dependencies = [
            "conda-forge::r-base>=4.1",
            "conda-forge::rpy2>=3.5",
            "numpy<2.0",
        ]
    if pip_requirements is None:
        pip_requirements = ["snowflake-connector-python"]

    # Add conda-forge::r-<pkg> deps for requested R packages
    r_conda_pkgs = []
    for p in r_packages:
        pkg_name = p if p.startswith("r-") else f"r-{p.lower()}"
        if not any(pkg_name in d for d in conda_dependencies):
            r_conda_pkgs.append(f"conda-forge::{pkg_name}")
    conda_dependencies = list(set(conda_dependencies + r_conda_pkgs))

    # Build the executor class
    ExecutorClass = _build_executor_class(
        partition_columns=partition_columns,
        r_packages=r_packages,
        r_entry_fn=entry_fn,
        output_columns=output_columns,
        diagnostic_mode=diagnostic_mode,
    )

    # Store R script as model artifact
    ctx = custom_model.ModelContext(artifacts={"r_script": r_script_path})
    model_instance = ExecutorClass(ctx)

    # Build signature
    input_features = _build_input_features(
        partition_columns, input_columns, model_signature
    )
    output_features = _build_output_features(
        output_columns, diagnostic_mode, model_signature
    )

    sig = model_signature.ModelSignature(
        inputs=input_features,
        outputs=output_features,
    )
    signatures = {"execute": sig, "execute_single": sig}

    # Registry
    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name
    reg = Registry(**reg_kwargs)

    if comment is None:
        comment = (
            f"Generic R executor | entry={entry_fn} | "
            f"partitions={partition_columns} | "
            f"diagnostic={'yes' if diagnostic_mode else 'no'}"
        )

    t0 = time.time()
    mv = reg.log_model(
        model_instance,
        model_name=model_name,
        version_name=version_name,
        conda_dependencies=conda_dependencies,
        pip_requirements=pip_requirements,
        target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
        options={
            "method_options": {
                "execute": {"function_type": "TABLE_FUNCTION"},
                "execute_single": {"function_type": "FUNCTION"},
            }
        },
        signatures=signatures,
        comment=comment,
    )
    reg_time = time.time() - t0

    return {
        "model_name": mv.model_name,
        "version_name": mv.version_name,
        "registration_time_s": round(reg_time, 1),
    }


def _build_input_features(partition_columns, input_columns, model_signature):
    """Build input FeatureSpec list."""
    features = []

    if input_columns:
        type_map = _get_type_map(model_signature)
        for col in input_columns:
            dtype = type_map.get(col["type"].upper(), model_signature.DataType.STRING)
            features.append(
                model_signature.FeatureSpec(name=col["name"], dtype=dtype)
            )
    else:
        for col in partition_columns:
            features.append(
                model_signature.FeatureSpec(
                    name=col, dtype=model_signature.DataType.STRING
                )
            )

    return features


def _build_output_features(output_columns, diagnostic_mode, model_signature):
    """Build output FeatureSpec list."""
    if output_columns and not diagnostic_mode:
        type_map = _get_type_map(model_signature)
        features = []
        existing_names = set()
        for col in output_columns:
            dtype = type_map.get(col["type"].upper(), model_signature.DataType.STRING)
            features.append(
                model_signature.FeatureSpec(name=col["name"], dtype=dtype)
            )
            existing_names.add(col["name"])
        # Add diagnostic columns only if not already declared
        if "PARTITION_KEY" not in existing_names:
            features.append(
                model_signature.FeatureSpec(
                    name="PARTITION_KEY", dtype=model_signature.DataType.STRING
                )
            )
        if "STATUS" not in existing_names:
            features.append(
                model_signature.FeatureSpec(
                    name="STATUS", dtype=model_signature.DataType.STRING
                )
            )
        if "ELAPSED_SEC" not in existing_names:
            features.append(
                model_signature.FeatureSpec(
                    name="ELAPSED_SEC", dtype=model_signature.DataType.DOUBLE
                )
            )
        return features

    # Default diagnostic schema
    return [
        model_signature.FeatureSpec(
            name="PARTITION_KEY", dtype=model_signature.DataType.STRING
        ),
        model_signature.FeatureSpec(
            name="STATUS", dtype=model_signature.DataType.STRING
        ),
        model_signature.FeatureSpec(
            name="ELAPSED_SEC", dtype=model_signature.DataType.DOUBLE
        ),
        model_signature.FeatureSpec(
            name="ROWS_IN", dtype=model_signature.DataType.INT64
        ),
        model_signature.FeatureSpec(
            name="ROWS_OUT", dtype=model_signature.DataType.INT64
        ),
        model_signature.FeatureSpec(
            name="ERROR_MSG", dtype=model_signature.DataType.STRING
        ),
        model_signature.FeatureSpec(
            name="RESULT_JSON", dtype=model_signature.DataType.STRING
        ),
    ]


def _get_type_map(model_signature):
    """Map string type names to ModelSignature DataType constants."""
    return {
        "STRING": model_signature.DataType.STRING,
        "VARCHAR": model_signature.DataType.STRING,
        "INT": model_signature.DataType.INT64,
        "INTEGER": model_signature.DataType.INT64,
        "INT64": model_signature.DataType.INT64,
        "DOUBLE": model_signature.DataType.DOUBLE,
        "FLOAT": model_signature.DataType.DOUBLE,
        "FLOAT64": model_signature.DataType.DOUBLE,
        "BOOLEAN": model_signature.DataType.BOOL,
        "BOOL": model_signature.DataType.BOOL,
    }
