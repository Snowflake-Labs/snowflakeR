# snowflakeR 0.2.0

## New modules

### Model Monitoring

Full model-monitoring lifecycle for deployed models:

- `sfr_monitor_source()` / `sfr_monitor_config()` -- configure
  monitoring sources and settings
- `sfr_add_monitor()` / `sfr_get_monitor()` /
  `sfr_show_model_monitors()` / `sfr_delete_monitor()` -- CRUD for
  monitors
- `sfr_monitor_drift()` / `sfr_monitor_performance()` /
  `sfr_monitor_stats()` -- retrieve monitoring results
- `sfr_suspend_monitor()` / `sfr_resume_monitor()` /
  `sfr_describe_monitor()` -- lifecycle management
- `sfr_add_monitor_segment()` / `sfr_drop_monitor_segment()` -- segment
  drill-down
- `sfr_monitor_to_vetiver()` / `sfr_vetiver_to_metrics()` -- vetiver
  integration bridge

### Experiment Tracking

MLflow-style experiment tracking on Snowflake:

- `sfr_experiment()` / `sfr_start_run()` / `sfr_end_run()` /
  `sfr_delete_run()` / `sfr_delete_experiment()` -- experiment lifecycle
- `sfr_exp_log_param()` / `sfr_exp_log_params()` -- parameter logging
- `sfr_exp_log_metric()` / `sfr_exp_log_metrics()` -- metric logging
- `sfr_exp_log_model()` / `sfr_exp_log_artifact()` -- artifact logging
- `sfr_exp_list_artifacts()` / `sfr_exp_download_artifact()` -- artifact
  retrieval
- `sfr_experiment_from_tune()` / `sfr_experiment_log_best()` -- `tune`
  grid-search integration

## New Feature Store functions

- `sfr_attach_feature_desc()` -- attach human-readable descriptions to
  individual features
- `sfr_slice_feature_view()` -- create a column-subset slice of a
  Feature View
- `sfr_fv_lineage()` -- trace upstream/downstream lineage from a Feature
  View
- `sfr_list_fv_columns()` -- list columns and types for a registered
  Feature View
- `sfr_fv_to_df()` -- read a Feature View's data as a data.frame
- `sfr_fv_query()` -- retrieve the underlying SQL query for a Feature
  View
- `sfr_fv_fqn()` -- get the fully qualified name of a Feature View
- `sfr_load_fvs_from_dataset()` -- recover Feature Views associated with
  a Dataset
- `sfr_update_default_warehouse()` -- change the default warehouse for a
  Feature Store
- `sfr_storage_config()` -- create Iceberg-backed storage configurations

## New Model Registry functions

- `sfr_delete_model_version()` -- delete a specific model version
- `sfr_get_model_metric()` -- read a single metric by name
- `sfr_delete_model_metric()` -- delete a single metric
- `sfr_model_description()` -- get or set the version description
- `sfr_show_model_functions()` -- list callable functions on a model
  version
- `sfr_model_lineage()` -- trace upstream/downstream model lineage
- `sfr_export_model()` -- export model artifacts to a local directory
- `sfr_get_model_task()` -- get the task type of a model version
- `sfr_list_services()` -- list active SPCS services for a model version
- `sfr_run_batch()` -- run batch inference via SPCS
- `sfr_models()` -- list Model objects in a registry (vs `sfr_show_models()`
  which returns a summary DataFrame)

## New parameters on existing functions

### Feature Store

- `sfr_feature_store()`: `default_iceberg_external_volume`
- `sfr_feature_view()` / `sfr_create_feature_view()`: `initialize`,
  `refresh_mode`, `cluster_by`, `online_config`
- `sfr_register_feature_view()`: `block`
- `sfr_read_feature_view()`: `store_type`, `keys`, `feature_names`
- `sfr_refresh_feature_view()`: `store_type`
- `sfr_get_refresh_history()`: `store_type`
- `sfr_generate_training_data()`: `exclude_columns`,
  `include_feature_view_timestamp_col`, `auto_prefix`, `join_method`
- `sfr_generate_dataset()`: `exclude_columns`,
  `include_feature_view_timestamp_col`, `auto_prefix`, `join_method`,
  `output_type`
- `sfr_retrieve_features()`: `exclude_columns`,
  `include_feature_view_timestamp_col`, `auto_prefix`, `join_method`

### Model Registry

- `sfr_log_model()`: `user_files`, `code_paths`, `resource_constraint`,
  `python_version`
- `sfr_predict()`: `partition_column`, `strict_input_validation`
- `sfr_deploy_model()`: `image_build_compute_pool`, `cpu_requests`,
  `memory_requests`, `gpu_requests`, `num_workers`, `max_batch_rows`,
  `block`, `build_external_access_integrations`

## Bug fixes

- `sfr_read_feature_view()`: Fixed `reticulate` type conversion for
  `keys` (R character vectors now correctly converted to Python list of
  lists of strings) and `feature_names` parameters.
- `sfr_fv_to_df()`: Worked around a `reticulate` `TypeError` on
  `cluster_by` field type inference by using `fs.read_feature_view()`
  instead of `fv.to_df()`.
- `sfr_slice_feature_view()`: Fixed `AttributeError` when the
  `FeatureViewSlice` object lacks `name`/`version` attributes; now falls
  back to the original request values.
- `sfr_models()`: Fixed `rbind` failure when model `comment` is `NULL`
  by coercing to empty string.
- `sfr_add_monitor_segment()` / `sfr_drop_monitor_segment()`: Added
  missing `segment` parameter in the Python bridge call.

## Documentation

- `DESCRIPTION` **Version** set to **0.2.0** (was 0.1.0), matching this
  changelog and the published feature set.
- Regenerated NAMESPACE and man/ pages for all new exports.
- Extended Feature Store and Model Registry vignettes with new sections
  covering online serving, aggregation, introspection, slicing, Iceberg,
  lineage, aliases, SQL-direct inference, batch inference, advanced
  deployment, and more.
- Updated README.md module overview table and example notebooks table.
- Added this NEWS.md changelog.
