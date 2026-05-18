
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='ip_upstate_s9_22', dag_params={})
lineage_task = LineageTask(dag_id='ip_upstate_s9_22', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='ip_upstate_s9_22',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['dev']
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 49,
            'flow_name': 'Ip_upstate_s9_22',
            'flow_key': 'ip_upstate_s9_22',
            'bh_project_id': 3,
            'project_name': 'Ingest_repo1',
            'flow_tags': [{'key': 'environment', 'value': 'dev'}],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
    )


    from airflow.operators.python import PythonOperator
    import os
    import re
    from airflow.hooks.base import BaseHook
    from airflow_plugins.cloud_factory import CloudFactory

    def validate_inbound_files(**context):
        params = context.get("params") or {}
        bucket_name = params.get("bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name is required for ValidateInboundFiles")

        source_prefix = (params.get("source_prefix") or "").lstrip("/")
        cloud_type = (params.get("cloud_type") or "aws").lower()
        if cloud_type not in ["aws", "gcp", "azure", "databricks"]:
            raise ValueError(f"Unsupported cloud_type '{cloud_type}' for ValidateInboundFiles")
        airflow_connection_id = params.get("airflow_connection_id", "aws_default")
        quarantine_prefix = params.get("quarantine_prefix")
        fail_on_invalid = params.get("fail_on_invalid", True)
        secret_name = params.get("secret_name")
        configured_sources = params.get("sources") or []
        ds_nodash = context.get("ds_nodash") or "unknown_date"

        if configured_sources and not isinstance(configured_sources, list):
            raise ValueError("sources must be an array when provided")

        # Backward compatibility path: build single-source definition from top-level fields.
        if not configured_sources:
            configured_sources = [{
                "source_name": "default",
                "prefix": source_prefix,
                "filename_regex": params.get("filename_regex"),
                "ignore_subfolders": False,
                "is_required": True,
                "required_patterns": [],
                "min_bytes": params.get("min_bytes", 1),
                "max_bytes": params.get("max_bytes"),
                "min_files": params.get("min_files", 1),
                "max_files": params.get("max_files"),
            }]

        conn = BaseHook.get_connection(airflow_connection_id)
        extras = conn.extra_dejson or {}
        factory_kwargs = {}
        if cloud_type == "aws":
            if conn.login:
                factory_kwargs["aws_access_key_id"] = conn.login
            if conn.password:
                factory_kwargs["aws_secret_access_key"] = conn.password
            factory_kwargs["region"] = (
                extras.get("region_name")
                or extras.get("region")
                or extras.get("aws_region")
                or "us-east-1"
            )
        elif cloud_type == "gcp":
            if extras.get("project"):
                factory_kwargs["project_id"] = extras.get("project")
            elif extras.get("project_id"):
                factory_kwargs["project_id"] = extras.get("project_id")
            if extras.get("keyfile_dict"):
                factory_kwargs["credentials"] = extras.get("keyfile_dict")
            elif extras.get("key_path"):
                factory_kwargs["credentials_path"] = extras.get("key_path")
        elif cloud_type == "azure":
            factory_kwargs["connection_string"] = (
                conn.password
                or extras.get("connection_string")
                or extras.get("azure_storage_connection_string")
            )
            if extras.get("account_url"):
                factory_kwargs["account_url"] = extras.get("account_url")
        elif cloud_type == "databricks":
            workspace_url = (conn.host or "").rstrip("/")
            token = conn.password
            if not workspace_url or not token:
                raise ValueError(
                    "Databricks connection must have host and password (token) for ValidateInboundFiles"
                )
            factory_kwargs["databricks_workspace_url"] = workspace_url
            factory_kwargs["databricks_token"] = token
            storage_secret_name = secret_name or extras.get("secret_name")
            if storage_secret_name:
                factory_kwargs["storage_secret_name"] = storage_secret_name
                factory_kwargs["storage_backend"] = "azure_blob"
            else:
                raise ValueError(
                    "secret_name is required for databricks validation storage. "
                    "Use value format 'scope/key' to avoid separate scope config."
                )
            if extras.get("secrets_scope_name"):
                factory_kwargs["secrets_scope_name"] = extras.get("secrets_scope_name")
            factory_kwargs["secrets_backend_type"] = extras.get("secrets_backend_type") or "databricks"

        factory = CloudFactory(cloud_type, **factory_kwargs)
        storage = factory.get_storage(cloud_type)
        valid_files = []
        invalid_files = []
        missing_required = []
        valid_keys_in_any_source = set()

        def _validate_numeric_rules(min_bytes, max_bytes, min_files, max_files):
            if min_bytes is not None and min_bytes < 0:
                raise ValueError("min_bytes must be >= 0")
            if max_bytes is not None and max_bytes < 0:
                raise ValueError("max_bytes must be >= 0")
            if min_bytes is not None and max_bytes is not None and min_bytes > max_bytes:
                raise ValueError("min_bytes cannot be greater than max_bytes")
            if min_files is not None and min_files < 0:
                raise ValueError("min_files must be >= 0")
            if max_files is not None and max_files < 1:
                raise ValueError("max_files must be >= 1")
            if min_files is not None and max_files is not None and min_files > max_files:
                raise ValueError("min_files cannot be greater than max_files")

        for source in configured_sources:
            source_name = (source or {}).get("source_name") or "default"
            source_rule_prefix = ((source or {}).get("prefix") or source_prefix).lstrip("/")
            filename_regex = (source or {}).get("filename_regex") or params.get("filename_regex")
            ignore_subfolders = bool((source or {}).get("ignore_subfolders", False))
            is_required = (source or {}).get("is_required", True)
            required_patterns = (source or {}).get("required_patterns") or []
            # Backward compatibility: if old required_patterns are present, preserve behavior.
            if required_patterns:
                required_regexes = [re.compile(pattern) for pattern in required_patterns]
            else:
                required_regexes = [re.compile(filename_regex)] if (is_required and filename_regex) else []
            min_bytes = (source or {}).get("min_bytes")
            if min_bytes is None:
                min_bytes = params.get("min_bytes", 1)
            max_bytes = (source or {}).get("max_bytes")
            if max_bytes is None:
                max_bytes = params.get("max_bytes")
            min_files = (source or {}).get("min_files")
            if min_files is None:
                min_files = params.get("min_files", 1)
            # Source optionality takes precedence over file-count minimum.
            if not is_required:
                min_files = 0
            max_files = (source or {}).get("max_files")
            if max_files is None:
                max_files = params.get("max_files")
            _validate_numeric_rules(min_bytes, max_bytes, min_files, max_files)

            regex = re.compile(filename_regex) if filename_regex else None

            objects_with_metadata = []
            if hasattr(storage, "list_objects_with_metadata"):
                objects_with_metadata = storage.list_objects_with_metadata(
                    bucket_name=bucket_name,
                    prefix=source_rule_prefix,
                ) or []
            if objects_with_metadata:
                candidate_files = [
                    {
                        "key": obj.get("key"),
                        "size_bytes": int(obj.get("size", obj.get("size_bytes", 0)) or 0),
                    }
                    for obj in objects_with_metadata
                    if obj.get("key") and not str(obj.get("key")).endswith("/")
                ]
            else:
                keys = storage.list_objects(bucket_name=bucket_name, prefix=source_rule_prefix) or []
                candidate_files = [
                    {"key": key, "size_bytes": 0}
                    for key in keys
                    if key and not key.endswith("/")
                ]

            valid_in_source = []
            invalid_in_source = []
            required_hits = [False for _ in required_regexes]
            for item in candidate_files:
                key = item["key"]
                base_name = os.path.basename(key)
                relative_key = key.lstrip("/")
                normalized_source_prefix = source_rule_prefix.strip("/")
                if normalized_source_prefix:
                    source_prefix_with_slash = f"{normalized_source_prefix}/"
                    if relative_key.startswith(source_prefix_with_slash):
                        relative_key = relative_key[len(source_prefix_with_slash):]
                reasons = []
                file_size = int(item.get("size_bytes", 0))
                regex_target = relative_key
                if ignore_subfolders and "/" in relative_key:
                    continue

                if regex and not regex.match(regex_target):
                    reasons.append(
                        f"filename '{regex_target}' does not match regex "
                        f"(ignore_subfolders={ignore_subfolders})"
                    )

                if min_bytes is not None and file_size < min_bytes:
                    reasons.append(f"file size {file_size} is below min_bytes {min_bytes}")
                if max_bytes is not None and file_size > max_bytes:
                    reasons.append(f"file size {file_size} exceeds max_bytes {max_bytes}")

                file_info = {
                    "source_name": source_name,
                    "key": key,
                    "size_bytes": file_size,
                    "base_name": base_name,
                    "relative_key": relative_key,
                }
                if reasons:
                    file_info["reasons"] = reasons
                    invalid_in_source.append(file_info)
                else:
                    valid_keys_in_any_source.add(key)
                    valid_in_source.append(file_info)
                    for idx, req_re in enumerate(required_regexes):
                        if req_re.match(regex_target):
                            required_hits[idx] = True

            valid_count = len(valid_in_source)
            total_count = len(candidate_files)
            if min_files is not None and valid_count < min_files:
                invalid_in_source.append({
                    "source_name": source_name,
                    "key": "__batch_rule__",
                    "size_bytes": 0,
                    "base_name": "__batch_rule__",
                    "reasons": [f"valid file count {valid_count} below min_files {min_files}"],
                })
            if max_files is not None and valid_count > max_files:
                invalid_in_source.append({
                    "source_name": source_name,
                    "key": "__batch_rule__",
                    "size_bytes": 0,
                    "base_name": "__batch_rule__",
                    "reasons": [f"valid file count {valid_count} above max_files {max_files}"],
                })

            required_pattern_labels = required_patterns or ([filename_regex] if (is_required and filename_regex) else [])
            for idx, pattern in enumerate(required_pattern_labels):
                if not required_hits[idx]:
                    rule_item = {
                        "source_name": source_name,
                        "required_pattern": pattern,
                        "reason": "required pattern not found",
                    }
                    missing_required.append(rule_item)
                    invalid_in_source.append({
                        "source_name": source_name,
                        "key": "__required_rule__",
                        "size_bytes": 0,
                        "base_name": "__required_rule__",
                        "reasons": [f"required pattern '{pattern}' not found"],
                    })

            valid_files.extend(valid_in_source)
            invalid_files.extend(invalid_in_source)

        quarantined_keys = []
        already_quarantined = set()
        filtered_invalid_files = []
        for item in invalid_files:
            key = item.get("key")
            if key in ("__batch_rule__", "__required_rule__") or key not in valid_keys_in_any_source:
                filtered_invalid_files.append(item)
        if quarantine_prefix and filtered_invalid_files:
            normalized_quarantine = quarantine_prefix.strip("/")
            for item in filtered_invalid_files:
                invalid_key = item.get("key")
                if not invalid_key or invalid_key in ("__batch_rule__", "__required_rule__"):
                    continue
                if invalid_key in valid_keys_in_any_source:
                    continue
                if invalid_key in already_quarantined:
                    continue
                target_key = f"{normalized_quarantine}/{ds_nodash}/{os.path.basename(invalid_key)}"
                moved = storage.move_object(
                    source_bucket_name=bucket_name,
                    source_object_key=invalid_key,
                    destination_bucket_name=bucket_name,
                    destination_object_key=target_key,
                )
                if not moved:
                    raise RuntimeError(f"Failed to quarantine '{invalid_key}' to '{target_key}'")
                already_quarantined.add(invalid_key)
                quarantined_keys.append(target_key)

        summary = {
            "cloud_type": cloud_type,
            "bucket_name": bucket_name,
            "source_prefix": source_prefix,
            "total_files": len(valid_files) + len([i for i in filtered_invalid_files if i.get("key") not in ("__batch_rule__", "__required_rule__")]),
            "valid_files": valid_files,
            "invalid_files": filtered_invalid_files,
            "missing_required": missing_required,
            "quarantined_keys": quarantined_keys,
        }
        context["ti"].xcom_push(key="valid_files", value=valid_files)
        context["ti"].xcom_push(key="invalid_files", value=filtered_invalid_files)
        context["ti"].xcom_push(key="missing_required", value=missing_required)
        context["ti"].xcom_push(key="validation_summary", value=summary)

        if fail_on_invalid and filtered_invalid_files:
            raise ValueError(
                f"ValidateInboundFiles found {len(filtered_invalid_files)} invalid items"
            )

        return summary

    _validate_params = {
        "bucket_name": "my-test-bucket",
        "source_prefix": "bhargs/3-Claims/3-Claims/Fidelis/upstate/IP",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-azureblob99",
        "filename_regex": None,
        "min_bytes": 1,
        "max_bytes": None,
        "min_files": 1,
        "max_files": None,
        "quarantine_prefix": None,
        "fail_on_invalid": False,
        "sources": [
            {
                "source_name": "safeharbor_fideliscare_prod_ipclaimheader_full_cinqupstate_20230101_20260331",
                "prefix": "bhargs/3-Claims/3-Claims/Fidelis/upstate/IP/"
            },
            {
                "source_name": "safeharbor_fideliscare_prod_ipclaimline_full_cinqupstate_20230101_20260331",
                "prefix": "bhargs/3-Claims/3-Claims/Fidelis/upstate/IP/"
            }
        ]
    }
    validate_inbound_files = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='validate_inbound_files',
        python_callable=validate_inbound_files,
        params=_validate_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.operators.python import PythonOperator
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    def create_databricks_cluster_create_compute(**context):
        from airflow_plugins.cloud_factory import CloudFactory
        hook = DatabricksHook(databricks_conn_id='databricks_default')
        conn = hook.get_conn()
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        payload = (
            {
                "cluster_name": "Databricks_1",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
                "num_workers": 0,
                "autoscale": None,
                "driver_node_type_id": None,
                "runtime_engine": None,
                "data_security_mode": None,
                "single_user_name": None,
                "policy_id": None,
                "apply_policy_default_values": True,
                "idempotency_token": None,
                "aws_attributes": None,
                "azure_attributes": None,
                "gcp_attributes": None,
                "single_node": False,
                "autotermination_minutes": 30,
                "enable_elastic_disk": True,
                "spark_conf": {},
                "spark_env_vars": {
                    "SECRET_MANAGER_PROVIDER": "databricks"
                },
                "custom_tags": {},
                "init_scripts": [],
                "libraries": [],
                "databricks_region": None,
                "bh_tags": [],
                "compute_config_name": "Databricks_1"
            }
        )
        cluster_id = compute.create_compute(
            payload,
            compute_name=payload.get("cluster_name"),
            run_async=False,
        )
        if not cluster_id:
            raise ValueError("create_compute did not return cluster_id")
        return cluster_id

    create_compute = PythonOperator(
        task_id='create_compute',
        python_callable=create_databricks_cluster_create_compute,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def submit_job_to_cluster(**context):
        params = context.get("params") or {}
        job_config = params.get("job_config")
        if not job_config:
            raise ValueError("Missing job_config in params")

        # arrives with literal { dag.dag_id }/{ ts_nodash }. Render it here.
        _job_name = job_config.get("name")
        if isinstance(_job_name, str) and "{" in _job_name:
            job_config = dict(job_config)
            job_config["name"] = context["task"].render_template(_job_name, context)

        # Prefer compute_id from params (supports Jinja xcom_pull strings), fallback to XCom.
        compute_id = params.get("compute_id")
        xcom_key = str(params.get("compute_xcom_key") or "return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            ti = context["ti"]
            # Most flows normalize the create task_id to 'create_compute'. Keep a legacy fallback.
            compute_task_id = params.get("compute_task_id") or "create_compute"
            compute_id = ti.xcom_pull(task_ids=compute_task_id, key=xcom_key)
            if not compute_id:
                compute_id = ti.xcom_pull(task_ids="databricks_create_cluster_task", key=xcom_key)

        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            raise ValueError("No compute_id from params or XCom")


        valid_files = params.get("valid_files")
        if isinstance(valid_files, str) and "{{" in valid_files:
            valid_files = context["task"].render_template(valid_files, context)
        if valid_files:
            import json
            import os
            from collections import defaultdict
            by_source = defaultdict(list)
            for f in valid_files:
                if not isinstance(f, dict):
                    continue
                key = f.get("key")
                if not key or str(key).startswith("__"):
                    continue
                src_name = (f.get("source_name") or "default").strip() or "default"
                rel = f.get("relative_key") or os.path.basename(str(key))
                by_source[src_name].append(str(rel).strip().lstrip("/"))
            overrides = {sn: ",".join(sorted(set(paths))) for sn, paths in by_source.items() if paths}
            if overrides:
                job_config = dict(job_config)
                args = list(job_config.get("parameters") or [])
                args.append(json.dumps(overrides, separators=(",", ":")))
                job_config["parameters"] = args

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        result = compute.execute_job(compute_id, job_config, run_async=False)
        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))
        run_id = result.get("run_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
        return result

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_pipelines_ip_upstate_s9_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/ip_upstate_s9_22/ip_upstate_s9.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}"
    }
    run_pipelines_ip_upstate_s9 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_ip_upstate_s9',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.operators.python import PythonOperator
    import os
    from airflow.hooks.base import BaseHook
    from airflow_plugins.cloud_factory import CloudFactory

    def archive_processed_files(**context):
        params = context.get("params") or {}
        bucket_name = params.get("bucket_name")
        archive_prefix = params.get("archive_prefix")
        cloud_type = (params.get("cloud_type") or "aws").lower()
        if not bucket_name:
            raise ValueError("bucket_name is required for ArchiveProcessedFiles")
        if not archive_prefix:
            raise ValueError("archive_prefix is required for ArchiveProcessedFiles")
        if cloud_type not in ["aws", "gcp", "azure", "databricks"]:
            raise ValueError(f"Unsupported cloud_type '{cloud_type}' for ArchiveProcessedFiles")

        input_files = params.get("files") or []
        source_prefix = (params.get("source_prefix") or "").lstrip("/")
        airflow_connection_id = params.get("airflow_connection_id", "aws_default")
        delete_source = params.get("delete_source", True)
        allow_empty = params.get("allow_empty", True)

        conn = BaseHook.get_connection(airflow_connection_id)
        extras = conn.extra_dejson or {}
        factory_kwargs = {}
        if cloud_type == "aws":
            if conn.login:
                factory_kwargs["aws_access_key_id"] = conn.login
            if conn.password:
                factory_kwargs["aws_secret_access_key"] = conn.password
            factory_kwargs["region"] = (
                extras.get("region_name")
                or extras.get("region")
                or extras.get("aws_region")
                or "us-east-1"
            )
        elif cloud_type == "gcp":
            if extras.get("project"):
                factory_kwargs["project_id"] = extras.get("project")
            elif extras.get("project_id"):
                factory_kwargs["project_id"] = extras.get("project_id")
            if extras.get("keyfile_dict"):
                factory_kwargs["credentials"] = extras.get("keyfile_dict")
            elif extras.get("key_path"):
                factory_kwargs["credentials_path"] = extras.get("key_path")
        elif cloud_type == "azure":
            factory_kwargs["connection_string"] = (
                conn.password
                or extras.get("connection_string")
                or extras.get("azure_storage_connection_string")
            )
            if extras.get("account_url"):
                factory_kwargs["account_url"] = extras.get("account_url")
        elif cloud_type == "databricks":
            workspace_url = (conn.host or "").rstrip("/")
            token = conn.password
            if not workspace_url or not token:
                raise ValueError(
                    "Databricks connection must have host and password (token) for ArchiveProcessedFiles"
                )
            factory_kwargs["databricks_workspace_url"] = workspace_url
            factory_kwargs["databricks_token"] = token
            # Keep archive task aligned with compute pattern: pass Databricks
            # secrets context to cloud-factory, let provider resolve backend creds.
            storage_backend = extras.get("storage_backend")
            if storage_backend:
                factory_kwargs["storage_backend"] = storage_backend
            account_url = extras.get("account_url")
            if account_url:
                factory_kwargs["account_url"] = account_url

            storage_secret_name = (
                params.get("secret_name")
                or extras.get("secret_name")
            )
            if storage_secret_name:
                factory_kwargs["storage_secret_name"] = storage_secret_name
                # Enforce Azure Blob path when secret-based storage is requested.
                factory_kwargs["storage_backend"] = "azure_blob"

            secrets_scope_name = extras.get("secrets_scope_name")
            if secrets_scope_name:
                factory_kwargs["secrets_scope_name"] = secrets_scope_name

            secrets_backend_type = extras.get("secrets_backend_type")
            if not secrets_backend_type:
                secrets_backend_type = "databricks"
            if secrets_backend_type:
                factory_kwargs["secrets_backend_type"] = secrets_backend_type

        factory = CloudFactory(cloud_type, **factory_kwargs)
        storage = factory.get_storage(cloud_type)
        ti = context["ti"]
        ds_nodash = context.get("ds_nodash") or "unknown_date"

        def _extract_source_key(item):
            if isinstance(item, str):
                return item
            if isinstance(item, dict):
                return (
                    item.get("key")
                    or item.get("source_key")
                    or item.get("object_key")
                    or item.get("path")
                )
            return None

        files_to_archive = []
        if isinstance(input_files, list):
            for item in input_files:
                source_key = _extract_source_key(item)
                if source_key:
                    files_to_archive.append(source_key)

        if not files_to_archive:
            files_to_archive = storage.list_objects(bucket_name=bucket_name, prefix=source_prefix) or []
            files_to_archive = [k for k in files_to_archive if k and not k.endswith("/")]

        if not files_to_archive and not allow_empty:
            raise ValueError("No files found to archive")

        archived_files = []
        for source_key in files_to_archive:
            base_name = os.path.basename(source_key)
            dest_key = f"{archive_prefix.strip('/')}/{ds_nodash}/{base_name}"
            if delete_source:
                moved = storage.move_object(
                    source_bucket_name=bucket_name,
                    source_object_key=source_key,
                    destination_bucket_name=bucket_name,
                    destination_object_key=dest_key,
                )
                if not moved:
                    raise RuntimeError(
                        f"Failed to move '{source_key}' to '{dest_key}' for cloud_type '{cloud_type}'"
                    )
            else:
                copied = storage.copy_object(
                    source_bucket_name=bucket_name,
                    source_object_key=source_key,
                    destination_bucket_name=bucket_name,
                    destination_object_key=dest_key,
                )
                if not copied:
                    raise RuntimeError(
                        f"Failed to copy '{source_key}' to '{dest_key}' for cloud_type '{cloud_type}'"
                    )
            archived_files.append({"source_key": source_key, "archive_key": dest_key})

        summary = {
            "cloud_type": cloud_type,
            "bucket_name": bucket_name,
            "archive_prefix": archive_prefix,
            "archived_count": len(archived_files),
            "delete_source": delete_source,
            "archived_files": archived_files,
        }
        ti.xcom_push(key="archived_files", value=archived_files)
        ti.xcom_push(key="archive_summary", value=summary)
        return summary

    _archive_params = {
        "bucket_name": "my-test-bucket",
        "files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "source_prefix": "",
        "archive_prefix": "bhargs/3-Claims/3-Claims/Fidelis/upstate/IP/archived/",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-azureblob99",
        "delete_source": True,
        "allow_empty": True
    }
    archive_processed_files = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='archive_processed_files',
        python_callable=archive_processed_files,
        params=_archive_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def terminate_databricks_resources(**context):
        ti = context["ti"]
        compute_id = ti.xcom_pull(task_ids="create_compute", key="return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            params = context.get("params") or {}
            compute_id = params.get("compute_id")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            logger.warning("No compute_id from XCom task create_compute or params; skipping terminate")
            return
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        ok = compute.terminate_compute(compute_id, run_async=False)
        logger.info("Terminated cluster %s: %s", compute_id, ok)

    _terminate_params = {}
    delete_compute = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute',
        python_callable=terminate_databricks_resources,
        params=_terminate_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        trigger_rule='all_done',
    )


    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> validate_inbound_files
    validate_inbound_files >> create_compute
    create_compute >> run_pipelines_ip_upstate_s9
    run_pipelines_ip_upstate_s9 >> archive_processed_files
    archive_processed_files >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
