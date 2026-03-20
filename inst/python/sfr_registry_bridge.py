"""
Snowflake Model Registry Bridge for R Models
=============================================

Python backend for snowflakeR::R/registry.R.

Architecture:
    R user code
        -> snowflakeR::R/registry.R  (user-facing R functions)
        -> reticulate bridge
        -> sfr_registry_bridge.py  (this file - Python plumbing)
        -> snowflake.ml.registry  (Snowflake ML Python SDK)

R users never import this directly - they use the sfr_* R functions which
call this module via reticulate.
"""

import contextlib
import io
import uuid
import textwrap
from typing import Dict, List, Optional, Any

import pandas as pd


def _quiet_call(fn, *args, **kwargs):
    """Run *fn* with stdout/stderr redirected to buffers.

    Prevents Snowpark's internal logging and SQL echo from reaching
    rpy2's C++ output handler, which crashes with basic_string::substr
    when strings exceed buffer boundaries.

    On success the captured output is discarded.  On failure the captured
    output is appended to the exception so diagnostics are preserved.
    """
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(out_buf), \
             contextlib.redirect_stderr(err_buf):
            return fn(*args, **kwargs)
    except Exception as e:
        captured = (err_buf.getvalue() + out_buf.getvalue()).strip()
        if captured:
            raise RuntimeError(
                f"{e}\n\n--- captured output ---\n{captured}"
            ) from e
        raise


def _pandas_to_r_dict(pdf):
    """Convert a pandas DataFrame to a column-oriented dict with native Python
    types via Series.tolist().  This avoids NumPy ABI issues with reticulate
    and is more efficient than the previous JSON round-trip approach.

    Datetime columns are converted to ISO-format strings to avoid ugly
    POSIX timestamp objects in R output.
    """
    import datetime

    _NA = "NA_SENTINEL_"
    clean_cols = [c.strip('"') for c in pdf.columns]
    pdf.columns = clean_cols
    cols = list(clean_cols)
    nrows = len(pdf)

    if nrows == 0:
        return {"columns": cols, "data": {c: [] for c in cols}, "nrows": 0}

    data = {}
    for col in cols:
        na_mask = pdf[col].isna()
        vals = pdf[col].tolist()

        # Convert datetime objects to ISO strings
        if vals and isinstance(vals[0], (datetime.datetime, datetime.date)):
            vals = [v.isoformat() if isinstance(v, (datetime.datetime, datetime.date)) else v for v in vals]

        if na_mask.any():
            data[col] = [_NA if is_na else (_NA if v is None else v)
                         for v, is_na in zip(vals, na_mask)]
        else:
            data[col] = [_NA if v is None else v for v in vals]

    return {"columns": cols, "data": data, "nrows": nrows}


# =============================================================================
# CustomModel Wrapper Factory
# =============================================================================

def _build_wrapper_class(
    predict_function: str,
    predict_packages: List[str],
    predict_body: Optional[str] = None,
    input_cols: Optional[Dict[str, str]] = None,
    output_cols: Optional[Dict[str, str]] = None,
):
    """
    Dynamically build a CustomModel subclass that wraps an R model.

    This factory creates a class that:
    - Loads the R model from an .rds file at init time (lazily)
    - On predict(), transfers data to R, calls the specified R function,
      and returns results as a pandas DataFrame

    Args:
        predict_function: R function name to call for inference
        predict_packages: R packages to load before calling predict
        predict_body: Optional custom R code for the predict body
        input_cols: Dict of {col_name: dtype} for input schema
        output_cols: Dict of {col_name: dtype} for output schema

    Returns:
        A CustomModel subclass ready for instantiation
    """
    from snowflake.ml.model import custom_model

    # Build the R code that will be executed for prediction
    if predict_body is not None:
        r_predict_code = predict_body
    elif predict_function == "forecast":
        r_predict_code = _build_forecast_r_code()
    elif predict_function == "predict":
        r_predict_code = _build_generic_predict_r_code(output_cols)
    else:
        r_predict_code = _build_custom_function_r_code(
            predict_function, output_cols
        )

    packages_to_load = list(predict_packages)

    class RModelWrapper(custom_model.CustomModel):
        """Auto-generated Python wrapper for an R model."""

        def __init__(self, context: custom_model.ModelContext):
            super().__init__(context)
            self._initialized = False
            self._r_model_name = f"r_model_{uuid.uuid4().hex[:8]}"

        def _ensure_initialized(self):
            if self._initialized:
                return

            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            from rpy2.robjects.conversion import localconverter

            # NOTE: We deliberately do NOT include numpy2ri.converter.
            # Importing / activating numpy2ri installs global hooks that
            # cause numpy's structured-array code to return recarrays
            # instead of plain ndarrays.  The SPCS inference server then
            # crashes at line 581 with "recarray has no attribute fillna"
            # because inference_df is a numpy.recarray rather than a
            # pandas.DataFrame.  pandas2ri alone is sufficient for
            # DataFrame <-> R data.frame conversion.
            combined = ro.default_converter + pandas2ri.converter

            with localconverter(combined):
                for pkg in packages_to_load:
                    ro.r(f"library({pkg})")

                model_path = self.context["model_rds"]
                ro.r(
                    f'{self._r_model_name} <- readRDS("{model_path}")'
                )

            self._initialized = True

        @custom_model.inference_api
        def predict(self, X: pd.DataFrame) -> pd.DataFrame:
            self._ensure_initialized()

            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            from rpy2.robjects.conversion import localconverter
            from rpy2.rinterface_lib.embedded import RRuntimeError

            # pandas2ri only -- see note in _ensure_initialized
            combined = ro.default_converter + pandas2ri.converter
            uid = uuid.uuid4().hex[:8]

            try:
                with localconverter(combined):
                    ro.globalenv[f"input_{uid}"] = X

                    full_r_code = r_predict_code.replace(
                        "{{MODEL}}", self._r_model_name
                    )
                    full_r_code = full_r_code.replace(
                        "{{INPUT}}", f"input_{uid}"
                    )
                    full_r_code = full_r_code.replace("{{UID}}", uid)
                    full_r_code = full_r_code.replace(
                        "{{N}}", f"nrow(input_{uid})"
                    )

                    ro.r(full_r_code)
                    result_df = ro.conversion.rpy2py(
                        ro.globalenv[f"result_{uid}"]
                    )
                    ro.r(f'rm(list = ls(pattern = "_{uid}$"))')

                # Ensure we always return a proper pandas DataFrame
                if not isinstance(result_df, pd.DataFrame):
                    result_df = pd.DataFrame(result_df)

                return result_df

            except RRuntimeError as e:
                try:
                    ro.r(f'rm(list = ls(pattern = "_{uid}$"))')
                except Exception:
                    pass
                raise RuntimeError(f"R execution error: {str(e)}")
            except Exception as e:
                try:
                    ro.r(f'rm(list = ls(pattern = "_{uid}$"))')
                except Exception:
                    pass
                raise RuntimeError(f"Prediction failed: {str(e)}")

    RModelWrapper.__name__ = f"RModelWrapper_{predict_function}"
    RModelWrapper.__qualname__ = RModelWrapper.__name__

    return RModelWrapper


def _build_forecast_r_code() -> str:
    """Build R code for forecast::forecast() prediction."""
    return textwrap.dedent("""\
        pred_{{UID}} <- forecast({{MODEL}}, h = {{N}})
        mean_{{UID}} <- as.numeric(pred_{{UID}}$mean)
        lower_{{UID}} <- as.matrix(pred_{{UID}}$lower)
        upper_{{UID}} <- as.matrix(pred_{{UID}}$upper)

        result_{{UID}} <- data.frame(
            period         = seq_len({{N}}),
            point_forecast = mean_{{UID}},
            lower_80       = lower_{{UID}}[, 1],
            upper_80       = upper_{{UID}}[, 1],
            lower_95       = lower_{{UID}}[, 2],
            upper_95       = upper_{{UID}}[, 2]
        )
    """)


def _build_generic_predict_r_code(
    output_cols: Optional[Dict[str, str]] = None,
) -> str:
    """Build R code for generic predict() call.

    Tries ``newdata`` first (base R convention) then falls back to
    ``new_data`` (tidymodels convention).  This order matters because
    predict.lm() silently ignores unrecognised arguments via ``...``
    and returns predictions on the training data instead of erroring.
    Tidymodels predict() explicitly rejects ``newdata`` with an error,
    so the fallback always fires for tidymodels workflows.

    Column names are preserved as-is from the inference server.  Since
    snowflakeR now preserves Snowflake's native UPPER-case column names
    throughout the pipeline (training data, model signature, inference
    input), no case conversion is needed here.
    """
    return textwrap.dedent("""\
        pred_{{UID}} <- tryCatch(
            predict({{MODEL}}, newdata = {{INPUT}}),
            error = function(e) {
                tryCatch(
                    predict({{MODEL}}, new_data = {{INPUT}}),
                    error = function(e2) {
                        msg <- conditionMessage(e2)
                        if (!nzchar(msg)) msg <- paste(utils::capture.output(print(e2)), collapse = "\\n")
                        stop(paste("predict() failed:", msg), call. = FALSE)
                    }
                )
            }
        )

        if (is.data.frame(pred_{{UID}})) {
            result_{{UID}} <- pred_{{UID}}
        } else if (is.matrix(pred_{{UID}})) {
            result_{{UID}} <- as.data.frame(pred_{{UID}})
        } else {
            result_{{UID}} <- data.frame(
                prediction = as.numeric(pred_{{UID}})
            )
        }
    """)


def _build_custom_function_r_code(
    func_name: str,
    output_cols: Optional[Dict[str, str]] = None,
) -> str:
    """Build R code for an arbitrary R function call."""
    return textwrap.dedent(f"""\
        pred_{{{{UID}}}} <- {func_name}({{{{MODEL}}}}, {{{{INPUT}}}})

        if (is.data.frame(pred_{{{{UID}}}})) {{
            result_{{{{UID}}}} <- pred_{{{{UID}}}}
        }} else if (is.matrix(pred_{{{{UID}}}})) {{
            result_{{{{UID}}}} <- as.data.frame(pred_{{{{UID}}}})
        }} else {{
            result_{{{{UID}}}} <- data.frame(
                prediction = as.numeric(pred_{{{{UID}}}})
            )
        }}
    """)


def _build_forecast_with_xreg_r_code() -> str:
    """Build R code for forecast with exogenous regressors."""
    return textwrap.dedent("""\
        xreg_{{UID}} <- as.matrix({{INPUT}})

        pred_{{UID}} <- forecast({{MODEL}}, xreg = xreg_{{UID}}, h = {{N}})
        mean_{{UID}} <- as.numeric(pred_{{UID}}$mean)
        lower_{{UID}} <- as.matrix(pred_{{UID}}$lower)
        upper_{{UID}} <- as.matrix(pred_{{UID}}$upper)

        result_{{UID}} <- data.frame(
            point_forecast = mean_{{UID}},
            lower_80       = lower_{{UID}}[, 1],
            upper_80       = upper_{{UID}}[, 1],
            lower_95       = lower_{{UID}}[, 2],
            upper_95       = upper_{{UID}}[, 2]
        )
    """)


# =============================================================================
# Model Signature Construction
# =============================================================================

_DTYPE_MAP = {
    "integer": "INT64",
    "int": "INT64",
    "int64": "INT64",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "float64": "DOUBLE",
    "numeric": "DOUBLE",
    "string": "STRING",
    "character": "STRING",
    "boolean": "BOOL",
    "logical": "BOOL",
    "bool": "BOOL",
}


def _build_signature(
    input_cols: Dict[str, str],
    output_cols: Dict[str, str],
) -> Any:
    """Construct a ModelSignature from column name -> type dicts."""
    from snowflake.ml.model.model_signature import (
        ModelSignature,
        FeatureSpec,
        DataType,
    )

    def _specs(cols: Dict[str, str]) -> List:
        specs = []
        for name, dtype_str in cols.items():
            dtype_key = dtype_str.lower().strip()
            if dtype_key not in _DTYPE_MAP:
                raise ValueError(
                    f"Unknown dtype '{dtype_str}' for column '{name}'. "
                    f"Valid types: {list(_DTYPE_MAP.keys())}"
                )
            dt = getattr(DataType, _DTYPE_MAP[dtype_key])
            specs.append(FeatureSpec(name=name, dtype=dt))
        return specs

    return ModelSignature(
        inputs=_specs(input_cols),
        outputs=_specs(output_cols),
    )


# =============================================================================
# Registry Operations (called from R via reticulate)
# =============================================================================

def registry_log_model(
    session,
    model_rds_path: str,
    model_name: str,
    version_name: Optional[str] = None,
    predict_function: str = "predict",
    predict_packages: Optional[List[str]] = None,
    predict_body: Optional[str] = None,
    input_cols: Optional[Dict[str, str]] = None,
    output_cols: Optional[Dict[str, str]] = None,
    conda_dependencies: Optional[List[str]] = None,
    pip_requirements: Optional[List[str]] = None,
    target_platforms: Optional[List[str]] = None,
    comment: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    sample_input: Optional[pd.DataFrame] = None,
    training_dataset_ref: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Log an R model to the Snowflake Model Registry."""
    from snowflake.ml.registry import Registry
    from snowflake.ml.model import custom_model

    if predict_packages is None:
        predict_packages = []
    if target_platforms is None:
        target_platforms = ["SNOWPARK_CONTAINER_SERVICES"]
    if conda_dependencies is None:
        # NOTE: The primary fix for the SPCS "recarray has no attribute
        # fillna" bug is removing numpy2ri from the rpy2 converter chain
        # (see _build_wrapper_class above).  We keep numpy<2.0 as a
        # belt-and-suspenders measure: it forces the conda solver to
        # pick Python 3.11 (matching pure-Python model containers)
        # rather than 3.12, which provides a known-good environment.
        # See: internal/ml_registry_issue/SPCS_INFERENCE_RECARRAY_BUG.md
        conda_dependencies = [
            "r-base>=4.1",
            "rpy2>=3.5",
            "numpy<2.0",
        ]

    has_rpy2 = any("rpy2" in dep for dep in conda_dependencies)
    if not has_rpy2:
        conda_dependencies.append("rpy2>=3.5")

    has_rbase = any("r-base" in dep for dep in conda_dependencies)
    if not has_rbase:
        conda_dependencies.insert(0, "r-base>=4.1")

    # Warn if no numpy pin -- container may resolve to numpy 2.x
    # which is known to break R model serving
    has_numpy_pin = any(
        "numpy" in dep and ("<" in dep or "==" in dep)
        for dep in conda_dependencies
    )
    if not has_numpy_pin:
        import warnings
        warnings.warn(
            "No numpy version pin found in conda_dependencies. "
            "Without 'numpy<2.0', r-base + rpy2 may resolve to "
            "Python 3.12 + numpy 2.x which causes a known SPCS "
            "inference server bug (recarray/fillna). Consider adding "
            "'numpy<2.0' to conda_dependencies.",
            stacklevel=2,
        )

    WrapperClass = _build_wrapper_class(
        predict_function=predict_function,
        predict_packages=predict_packages,
        predict_body=predict_body,
        input_cols=input_cols,
        output_cols=output_cols,
    )

    model_context = custom_model.ModelContext(model_rds=model_rds_path)
    model_wrapper = WrapperClass(model_context)

    signatures = None
    if input_cols and output_cols:
        sig = _build_signature(input_cols, output_cols)
        signatures = {"predict": sig}

    if sample_input is None and input_cols:
        sample_rows = {
            name: (
                [1]
                if dtype.lower() in ("integer", "int", "int64")
                else [1.0]
                if dtype.lower()
                in ("double", "float", "float64", "numeric")
                else ["a"]
                if dtype.lower() in ("string", "character")
                else [True]
            )
            for name, dtype in input_cols.items()
        }
        sample_input = pd.DataFrame(sample_rows)
    elif sample_input is not None:
        pass  # preserve column names as-is

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)

    # If a training dataset reference is provided, use its Snowpark DataFrame
    # as sample_input_data to complete Feature View -> Dataset -> Model lineage.
    lineage_sample = None
    if training_dataset_ref:
        ds_name = training_dataset_ref.get("name")
        ds_version = training_dataset_ref.get("version")
        if ds_name and ds_version:
            from sfr_features_bridge import get_cached_dataset
            ds = get_cached_dataset(ds_name, ds_version)
            if ds is not None:
                lineage_sample = ds.read.to_snowpark_dataframe()
            else:
                print(
                    f"[snowflakeR] Dataset '{ds_name}:{ds_version}' not in "
                    "cache; falling back to pandas sample_input_data."
                )

    log_kwargs = {
        "model": model_wrapper,
        "model_name": model_name,
        "conda_dependencies": conda_dependencies,
        "target_platforms": target_platforms,
    }

    if version_name:
        log_kwargs["version_name"] = version_name
    if signatures:
        log_kwargs["signatures"] = signatures
    if lineage_sample is not None:
        log_kwargs["sample_input_data"] = lineage_sample
    elif sample_input is not None:
        log_kwargs["sample_input_data"] = sample_input
    if comment:
        log_kwargs["comment"] = comment
    if metrics:
        log_kwargs["metrics"] = metrics
    if pip_requirements:
        log_kwargs["pip_requirements"] = pip_requirements
    if options:
        log_kwargs["options"] = options

    mv = _quiet_call(reg.log_model, **log_kwargs)

    return {
        "success": True,
        "model_name": mv.model_name,
        "version_name": mv.version_name,
        "model_version": mv,
        "registry": reg,
    }


def registry_show_models(
    session,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> pd.DataFrame:
    """List models in the registry."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    return _pandas_to_r_dict(reg.show_models())


def registry_get_model(
    session,
    model_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Get a model reference from the registry."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)

    return {
        "model": m,
        "name": model_name,
        "comment": m.comment,
        "versions": [v.version_name for v in m.versions()],
        "default_version": (
            m.default.version_name if m.default else None
        ),
        "registry": reg,
    }


def registry_show_versions(
    session,
    model_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> pd.DataFrame:
    """Show versions of a model."""
    info = registry_get_model(
        session, model_name, database_name, schema_name
    )
    return _pandas_to_r_dict(info["model"].show_versions())


def registry_predict(
    session,
    model_name: str,
    version_name: Optional[str] = None,
    input_data: Optional[pd.DataFrame] = None,
    input_data_path: Optional[str] = None,
    function_name: str = "predict",
    service_name: Optional[str] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> str:
    """Run inference using a registered model.

    Input data can be passed as a pandas DataFrame (``input_data``) or as
    a path to a CSV file (``input_data_path``).  The CSV path avoids the
    ``basic_string::substr`` C++ crash in rpy2 that occurs when
    ``reticulate::r_to_py()`` converts R data.frames with string columns.

    Returns the path to a temp JSON file containing the result (same
    workaround, output direction).
    """
    import json
    import tempfile

    from snowflake.ml.registry import Registry

    if input_data_path is not None:
        input_data = pd.read_csv(input_data_path)

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)

    if version_name:
        mv = m.version(version_name)
    else:
        mv = m.default

    sp_df = session.create_dataframe(input_data)

    run_kwargs = {"function_name": function_name}
    if service_name:
        run_kwargs["service_name"] = service_name

    def _run_and_collect():
        r = mv.run(sp_df, **run_kwargs)
        return r.to_pandas()

    result_df = _quiet_call(_run_and_collect)
    result_df.columns = [c.strip('"') for c in result_df.columns]

    d = _pandas_to_r_dict(result_df)

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, prefix="sfr_predict_"
    )
    json.dump(d, tmp)
    tmp.close()
    return tmp.name


def registry_predict_local(
    model_rds_path: str,
    input_data: pd.DataFrame,
    predict_function: str = "predict",
    predict_packages: Optional[List[str]] = None,
    predict_body: Optional[str] = None,
    input_cols: Optional[Dict[str, str]] = None,
    output_cols: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Test an R model locally without deploying to Snowflake."""
    from snowflake.ml.model import custom_model

    if predict_packages is None:
        predict_packages = []

    WrapperClass = _build_wrapper_class(
        predict_function=predict_function,
        predict_packages=predict_packages,
        predict_body=predict_body,
        input_cols=input_cols,
        output_cols=output_cols,
    )

    ctx = custom_model.ModelContext(model_rds=model_rds_path)
    wrapper = WrapperClass(ctx)

    return _pandas_to_r_dict(wrapper.predict(input_data))


def registry_delete_model(
    session,
    model_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> bool:
    """Delete a model from the registry."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    _quiet_call(reg.delete_model, model_name)
    return True


def registry_delete_version(
    session,
    model_name: str,
    version_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> bool:
    """Delete a specific version of a model."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)
    m.delete_version(version_name)
    return True


def registry_set_metric(
    session,
    model_name: str,
    version_name: str,
    metric_name: str,
    metric_value: Any,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> bool:
    """Set a metric on a model version."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)
    mv = m.version(version_name)
    mv.set_metric(metric_name, metric_value)
    return True


def registry_show_metrics(
    session,
    model_name: str,
    version_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Get metrics for a model version."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)
    mv = m.version(version_name)
    return mv.show_metrics()


def registry_set_default_version(
    session,
    model_name: str,
    version_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> bool:
    """Set the default version of a model."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)
    m.default = version_name
    return True


# =============================================================================
# SPCS Service Management
# =============================================================================

def registry_create_service(
    session,
    model_name: str,
    version_name: str,
    service_name: str,
    compute_pool: str,
    image_repo: str,
    ingress_enabled: bool = True,
    max_instances: int = 1,
    force: bool = False,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Deploy a model version as an SPCS service.

    If *force* is True and the service already exists, it is dropped first.
    """
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)
    mv = m.version(version_name)

    if force:
        # Drop via SQL -- mv.delete_service() is version-scoped and won't
        # find services created by a different version of the same model.
        svc_fqn = f"{database_name}.{schema_name}.{service_name}" if database_name and schema_name else service_name
        try:
            session.sql(f"DROP SERVICE IF EXISTS {svc_fqn}").collect()
        except Exception:
            pass

    _quiet_call(
        mv.create_service,
        service_name=service_name,
        service_compute_pool=compute_pool,
        image_repo=image_repo,
        ingress_enabled=ingress_enabled,
        max_instances=max_instances,
    )

    return {
        "success": True,
        "service_name": service_name,
        "compute_pool": compute_pool,
        "model_name": model_name,
        "version_name": version_name,
    }


def registry_delete_service(
    session,
    model_name: str,
    version_name: str,
    service_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> bool:
    """Delete an SPCS service for a model version."""
    from snowflake.ml.registry import Registry

    reg_kwargs = {"session": session}
    if database_name:
        reg_kwargs["database_name"] = database_name
    if schema_name:
        reg_kwargs["schema_name"] = schema_name

    reg = Registry(**reg_kwargs)
    m = reg.get_model(model_name)
    mv = m.version(version_name)
    _quiet_call(mv.delete_service, service_name)
    return True


# =============================================================================
# Snowpark Session Helper
# =============================================================================

def get_session():
    """Get the current Snowpark session (Workspace Notebooks)."""
    from snowflake.snowpark.context import get_active_session
    return get_active_session()


# =============================================================================
# Built-in predict templates
# =============================================================================

PREDICT_TEMPLATES = {
    "forecast": _build_forecast_r_code(),
    "forecast_xreg": _build_forecast_with_xreg_r_code(),
    "predict": _build_generic_predict_r_code(),
}


def list_predict_templates() -> Dict[str, str]:
    """Return available prediction code templates."""
    return {k: v for k, v in PREDICT_TEMPLATES.items()}
