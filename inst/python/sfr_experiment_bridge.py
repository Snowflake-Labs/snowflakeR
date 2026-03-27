"""
Snowflake Experiment Tracking Bridge for R
==========================================

Python backend for snowflakeR::R/experiments.R.

The ExperimentTracking class is a singleton -- only one active experiment/run
context at a time. This module manages the global instance.
"""

import importlib.metadata

try:
    _ML_VERSION_STR = importlib.metadata.version("snowflake-ml-python")
    _ML_VERSION = tuple(int(x) for x in _ML_VERSION_STR.split(".")[:3])
except Exception:
    _ML_VERSION_STR = "unknown"
    _ML_VERSION = (0, 0, 0)


def _requires(min_version, feature_name):
    """Raise RuntimeError if the installed snowflake-ml-python is too old."""
    if _ML_VERSION < min_version:
        need = ".".join(str(x) for x in min_version)
        raise RuntimeError(
            f"{feature_name} requires snowflake-ml-python >= {need}, "
            f"you have {_ML_VERSION_STR}. "
            f"Upgrade: pip install 'snowflake-ml-python>={need}'"
        )


_EXP_INSTANCE = None


def _get_or_create_exp(session, database_name=None, schema_name=None):
    global _EXP_INSTANCE
    _requires((1, 19, 0), "Experiment Tracking")
    from snowflake.ml.experiment import ExperimentTracking
    if _EXP_INSTANCE is None:
        _EXP_INSTANCE = ExperimentTracking(session=session)
    return _EXP_INSTANCE


def set_experiment(session, name, database_name=None, schema_name=None):
    exp = _get_or_create_exp(session, database_name, schema_name)
    exp.set_experiment(name)
    return {"name": name, "status": "active"}


def start_run(session, run_name=None):
    exp = _get_or_create_exp(session)
    exp.start_run(run_name)
    return {"run_name": run_name, "status": "running"}


def end_run(session, run_name=None):
    exp = _get_or_create_exp(session)
    exp.end_run(run_name)
    return {"run_name": run_name, "status": "completed"}


def log_param(session, key, value):
    exp = _get_or_create_exp(session)
    exp.log_param(key, value)


def log_params(session, params_dict):
    exp = _get_or_create_exp(session)
    exp.log_params(params_dict)


def log_metric(session, key, value, step=0):
    exp = _get_or_create_exp(session)
    exp.log_metric(key, value, step=step)


def log_metrics(session, metrics_dict, step=0):
    exp = _get_or_create_exp(session)
    exp.log_metrics(metrics_dict, step=step)


def log_model(session, model, model_name, signatures=None, sample_input_data=None):
    exp = _get_or_create_exp(session)
    kwargs = {"model_name": model_name}
    if signatures:
        kwargs["signatures"] = signatures
    if sample_input_data is not None:
        kwargs["sample_input_data"] = sample_input_data
    exp.log_model(model, **kwargs)


def log_artifact(session, local_path, artifact_path=None):
    exp = _get_or_create_exp(session)
    kwargs = {}
    if artifact_path:
        kwargs["artifact_path"] = artifact_path
    exp.log_artifact(local_path, **kwargs)


def list_artifacts(session, run_name, artifact_path=None):
    exp = _get_or_create_exp(session)
    kwargs = {}
    if artifact_path:
        kwargs["artifact_path"] = artifact_path
    return exp.list_artifacts(run_name, **kwargs)


def download_artifacts(session, run_name, artifact_path, target_path):
    exp = _get_or_create_exp(session)
    exp.download_artifacts(run_name, artifact_path=artifact_path,
                           target_path=target_path)


def delete_run(session, experiment_name, run_name):
    exp = _get_or_create_exp(session)
    exp.set_experiment(experiment_name)
    exp.delete_run(run_name)
    return {"run_name": run_name, "status": "deleted"}


def delete_experiment(session, name):
    exp = _get_or_create_exp(session)
    exp.delete_experiment(name)
    return {"name": name, "status": "deleted"}


def reset_instance():
    """Reset the singleton for testing purposes."""
    global _EXP_INSTANCE
    _EXP_INSTANCE = None
