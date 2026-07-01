
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask
import airflow_plugins.dag_task_definitions.feed_control_callbacks as feed_control_callbacks

common_task = CommonTask(dag_id='sample_test7799_652_626', dag_params={})
lineage_task = LineageTask(dag_id='sample_test7799_652_626', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='sample_test7799_652_626',
    default_args=default_args,
    schedule='0 0 1 * *',
    catchup=False,
    tags=['dev']
    params={
  "bh_environment_name": "kiran_databricks",
  "bh_environment_id": 321,
  "bh_kc_secret_url": "bh-dev-westus3-kv-key-scope/bh-app-muthukumar-databricks-keycloak-321-v1-secrets",
  "cloud_provider": "databricks"
}
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 626,
            'flow_name': 'sample_test7799_652',
            'flow_key': 'sample_test7799_652',
            'bh_project_id': 303,
            'project_name': 'Project_s1',
            'flow_tags': [{'key': 'environment', 'value': 'dev'}],
            'flow_type': 'INGESTION',
            'tenant_id': 4,
            'flow_status': 'In Progress',
        }
    )


    from airflow.operators.python import PythonOperator
    from airflow_plugins.feed_control.sla.inbound.validate_task import (
        validate_inbound_files,
    )

    _validate_params = {
        "bucket_name": "my-test-bucket",
        "source_prefix": "testing",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-azureblobconn",
        "filename_regex": None,
        "min_bytes": 1,
        "max_bytes": None,
        "min_files": 1,
        "max_files": None,
        "quarantine_prefix": "testing/rejected",
        "fail_on_invalid": True,
        "sources": [
            {
                "source_name": "employees",
                "prefix": "testing",
                "filename_regex": "employees.csv",
                "ignore_subfolders": True,
                "is_required": True,
                "min_bytes": 10,
                "min_files": 1
            }
        ],
        "allow_control_table_failure": False,
        "require_batch_id": None,
        "require_feed_control_policy": True,
        "control_catalog": None,
        "control_schema": None,
        "feed_name": "sample_test7799"
    }
    validate_inbound_files = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='validate_inbound_files',
        python_callable=validate_inbound_files,
        params=_validate_params,
        on_success_callback=feed_control_callbacks.validate_inbound_success_callback,
        on_failure_callback=feed_control_callbacks.validate_inbound_failure_callback,
    )


    from airflow.operators.python import PythonOperator
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    def create_databricks_cluster_create_compute(**context):
        from airflow_plugins.cloud_factory import CloudFactory
        hook = DatabricksHook(databricks_conn_id='databricks_default')
        conn = hook.get_conn()
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")
        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        payload = (
            {
                "cluster_name": "New_compute_99",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D16ds_v5",
                "num_workers": 0,
                "autoscale": None,
                "driver_node_type_id": None,
                "runtime_engine": None,
                "data_security_mode": "SINGLE_USER",
                "single_user_name": "sathish@bighammer.ai",
                "policy_id": None,
                "apply_policy_default_values": True,
                "idempotency_token": None,
                "aws_attributes": None,
                "azure_attributes": None,
                "gcp_attributes": None,
                "single_node": True,
                "autotermination_minutes": 30,
                "enable_elastic_disk": True,
                "spark_conf": {},
                "spark_env_vars": {
                    "SECRET_MANAGER_PROVIDER": "databricks",
                    "PIPELINE_EXECUTION_MODE": "parallel",
                    "PIPELINE_PARALLEL_MAX_WORKERS": "12"
                },
                "custom_tags": {},
                "init_scripts": [
                    "/Workspace/Shared/dev-utils/scripts/bh_databricks_grpc_server.sh"
                ],
                "libraries": [],
                "databricks_region": "us-west-2",
                "bh_tags": []
            }
        )
        cluster_id = compute.create_compute(
            payload,
            compute_name=payload.get("cluster_name"),
            run_async=False,
        )
        if not cluster_id:
            raise ValueError("create_compute did not return cluster_id")

        num_workers = payload.get("num_workers", 0)
        context["ti"].xcom_push(key="bh_audit_metadata", value={
            "databricks_cluster_id": cluster_id,
            "databricks_cluster_size": num_workers,
            "databricks_user_account": user_account,
            "ingestion_group_id": 652,
            "flow_id": 626
        })
        return cluster_id

    create_compute = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
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
            if isinstance(valid_files, str):
                import ast
                try:
                    valid_files = ast.literal_eval(valid_files)
                except (ValueError, SyntaxError):
                    logger.warning("Rendered valid_files is not valid Python literal", extra={"rendered_valid_files": valid_files})
                    valid_files = None
        if valid_files:
            import json
            from collections import defaultdict
            by_source = defaultdict(list)
            for f in valid_files:
                if not isinstance(f, dict):
                    continue
                key = f.get("key")
                if not key or str(key).startswith("__"):
                    continue
                src_name = (f.get("source_name") or "default").strip() or "default"
                by_source[src_name].append(str(key).strip().lstrip("/"))
            overrides = {sn: ",".join(sorted(set(paths))) for sn, paths in by_source.items() if paths}
            if overrides:
                job_config = dict(job_config)
                args = list(job_config.get("parameters") or [])
                args.append(json.dumps(overrides, separators=(",", ":")))
                job_config["parameters"] = args


        batch_id = params.get("batch_id")
        if isinstance(batch_id, str) and "{{" in batch_id:
            batch_id = context["task"].render_template(batch_id, context)
        if batch_id is not None:
            job_config = dict(job_config)
            args = list(job_config.get("parameters") or [])
            # main.py expects batch_id as 5th positional argument.
            # Ensure 4th positional (runtime_parameters_json) exists first.
            if len(args) <= 3:
                args.append("{}")
            args.append(str(batch_id))
            job_config["parameters"] = args

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        audit_meta = {
            "databricks_cluster_id": compute_id,
            "databricks_user_account": user_account
        }
        # Audit context for the submit_job event: ingestion_group_id, flow_id, pipeline_id.
        for _audit_k in ("ingestion_group_id", "flow_id", "pipeline_id"):
            if params.get(_audit_k) is not None:
                audit_meta[_audit_k] = params.get(_audit_k)

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        try:
            _cfg = compute.get_compute_configuration(compute_id)
            _size = _cfg.get("num_workers")
            if _size is not None:
                audit_meta["databricks_cluster_size"] = _size
        except Exception as _e:
            logger.warning("Could not resolve cluster size for %s: %s", compute_id, _e)
        result = compute.execute_job(compute_id, job_config, run_async=False)

        run_id = result.get("run_id")
        job_id = result.get("job_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
            audit_meta["databricks_run_id"] = run_id
        if job_id:
            audit_meta["databricks_job_id"] = job_id
        run_url = result.get("run_page_url")
        if not run_url and run_id:
            _job_id = result.get("job_id")
            if _job_id:
                run_url = workspace_url + "/jobs/" + str(_job_id) + "/runs/" + str(run_id)
            else:
                run_url = workspace_url + "/jobs/runs/" + str(run_id)
        if run_url:
            context["ti"].xcom_push(key="databricks_run_url", value=run_url)
            audit_meta["databricks_run_url"] = run_url
        context["ti"].xcom_push(key="bh_audit_metadata", value=audit_meta)

        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))

        if params.get("feed_name") or params.get("feed_id"):
            from airflow_plugins.dag_task_definitions.feed_control_callbacks import (
                run_post_submit_feed_control_or_fail,
            )
            run_post_submit_feed_control_or_fail(context)

        return result

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_pipelines_employees_652_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=303/pipeline/pipeline_id=1142/employees_652.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 652,
        "flow_id": 626,
        "pipeline_id": 1142,
        "feed_name": "sample_test7799",
        "validate_inbound_task_id": "validate_inbound_files",
        "facts_source": "databricks",
        "pipeline_name": "employees_652",
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "batch_id": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='batch_id') }}"
    }
    run_pipelines_employees_652 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_employees_652',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
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
            if isinstance(valid_files, str):
                import ast
                try:
                    valid_files = ast.literal_eval(valid_files)
                except (ValueError, SyntaxError):
                    logger.warning("Rendered valid_files is not valid Python literal", extra={"rendered_valid_files": valid_files})
                    valid_files = None
        if valid_files:
            import json
            from collections import defaultdict
            by_source = defaultdict(list)
            for f in valid_files:
                if not isinstance(f, dict):
                    continue
                key = f.get("key")
                if not key or str(key).startswith("__"):
                    continue
                src_name = (f.get("source_name") or "default").strip() or "default"
                by_source[src_name].append(str(key).strip().lstrip("/"))
            overrides = {sn: ",".join(sorted(set(paths))) for sn, paths in by_source.items() if paths}
            if overrides:
                job_config = dict(job_config)
                args = list(job_config.get("parameters") or [])
                args.append(json.dumps(overrides, separators=(",", ":")))
                job_config["parameters"] = args


        batch_id = params.get("batch_id")
        if isinstance(batch_id, str) and "{{" in batch_id:
            batch_id = context["task"].render_template(batch_id, context)
        if batch_id is not None:
            job_config = dict(job_config)
            args = list(job_config.get("parameters") or [])
            # main.py expects batch_id as 5th positional argument.
            # Ensure 4th positional (runtime_parameters_json) exists first.
            if len(args) <= 3:
                args.append("{}")
            args.append(str(batch_id))
            job_config["parameters"] = args

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        audit_meta = {
            "databricks_cluster_id": compute_id,
            "databricks_user_account": user_account
        }
        # Audit context for the submit_job event: ingestion_group_id, flow_id, pipeline_id.
        for _audit_k in ("ingestion_group_id", "flow_id", "pipeline_id"):
            if params.get(_audit_k) is not None:
                audit_meta[_audit_k] = params.get(_audit_k)

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        try:
            _cfg = compute.get_compute_configuration(compute_id)
            _size = _cfg.get("num_workers")
            if _size is not None:
                audit_meta["databricks_cluster_size"] = _size
        except Exception as _e:
            logger.warning("Could not resolve cluster size for %s: %s", compute_id, _e)
        result = compute.execute_job(compute_id, job_config, run_async=False)

        run_id = result.get("run_id")
        job_id = result.get("job_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
            audit_meta["databricks_run_id"] = run_id
        if job_id:
            audit_meta["databricks_job_id"] = job_id
        run_url = result.get("run_page_url")
        if not run_url and run_id:
            _job_id = result.get("job_id")
            if _job_id:
                run_url = workspace_url + "/jobs/" + str(_job_id) + "/runs/" + str(run_id)
            else:
                run_url = workspace_url + "/jobs/runs/" + str(run_id)
        if run_url:
            context["ti"].xcom_push(key="databricks_run_url", value=run_url)
            audit_meta["databricks_run_url"] = run_url
        context["ti"].xcom_push(key="bh_audit_metadata", value=audit_meta)

        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))

        if params.get("feed_name") or params.get("feed_id"):
            from airflow_plugins.dag_task_definitions.feed_control_callbacks import (
                run_post_submit_feed_control_or_fail,
            )
            run_post_submit_feed_control_or_fail(context)

        return result

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_pipelines_silver_raw_employees_to_target_employees_map_260701_9f15_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=303/pipeline/pipeline_id=1143/silver_raw_employees_to_target_employees_map_260701_9f15.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 652,
        "flow_id": 626,
        "pipeline_id": 1143,
        "feed_name": "sample_test7799",
        "validate_inbound_task_id": "validate_inbound_files",
        "facts_source": "databricks",
        "pipeline_name": "silver_raw_employees_to_target_employees_map_260701_9f15",
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "batch_id": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='batch_id') }}"
    }
    run_pipelines_silver_raw_employees_to_target_employees_map_260701_9f15 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_silver_raw_employees_to_target_employees_map_260701_9f15',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
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
            if isinstance(valid_files, str):
                import ast
                try:
                    valid_files = ast.literal_eval(valid_files)
                except (ValueError, SyntaxError):
                    logger.warning("Rendered valid_files is not valid Python literal", extra={"rendered_valid_files": valid_files})
                    valid_files = None
        if valid_files:
            import json
            from collections import defaultdict
            by_source = defaultdict(list)
            for f in valid_files:
                if not isinstance(f, dict):
                    continue
                key = f.get("key")
                if not key or str(key).startswith("__"):
                    continue
                src_name = (f.get("source_name") or "default").strip() or "default"
                by_source[src_name].append(str(key).strip().lstrip("/"))
            overrides = {sn: ",".join(sorted(set(paths))) for sn, paths in by_source.items() if paths}
            if overrides:
                job_config = dict(job_config)
                args = list(job_config.get("parameters") or [])
                args.append(json.dumps(overrides, separators=(",", ":")))
                job_config["parameters"] = args


        batch_id = params.get("batch_id")
        if isinstance(batch_id, str) and "{{" in batch_id:
            batch_id = context["task"].render_template(batch_id, context)
        if batch_id is not None:
            job_config = dict(job_config)
            args = list(job_config.get("parameters") or [])
            # main.py expects batch_id as 5th positional argument.
            # Ensure 4th positional (runtime_parameters_json) exists first.
            if len(args) <= 3:
                args.append("{}")
            args.append(str(batch_id))
            job_config["parameters"] = args

        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('databricks_default')
        workspace_url = (conn.host or '').rstrip('/')
        token = conn.password
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        audit_meta = {
            "databricks_cluster_id": compute_id,
            "databricks_user_account": user_account
        }
        # Audit context for the submit_job event: ingestion_group_id, flow_id, pipeline_id.
        for _audit_k in ("ingestion_group_id", "flow_id", "pipeline_id"):
            if params.get(_audit_k) is not None:
                audit_meta[_audit_k] = params.get(_audit_k)

        factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
        compute = factory.get_compute(compute_type="databricks")
        try:
            _cfg = compute.get_compute_configuration(compute_id)
            _size = _cfg.get("num_workers")
            if _size is not None:
                audit_meta["databricks_cluster_size"] = _size
        except Exception as _e:
            logger.warning("Could not resolve cluster size for %s: %s", compute_id, _e)
        result = compute.execute_job(compute_id, job_config, run_async=False)

        run_id = result.get("run_id")
        job_id = result.get("job_id")
        if run_id:
            context["ti"].xcom_push(key="run_id", value=run_id)
            audit_meta["databricks_run_id"] = run_id
        if job_id:
            audit_meta["databricks_job_id"] = job_id
        run_url = result.get("run_page_url")
        if not run_url and run_id:
            _job_id = result.get("job_id")
            if _job_id:
                run_url = workspace_url + "/jobs/" + str(_job_id) + "/runs/" + str(run_id)
            else:
                run_url = workspace_url + "/jobs/runs/" + str(run_id)
        if run_url:
            context["ti"].xcom_push(key="databricks_run_url", value=run_url)
            audit_meta["databricks_run_url"] = run_url
        context["ti"].xcom_push(key="bh_audit_metadata", value=audit_meta)

        if result.get("status") == "FAILED":
            raise RuntimeError(result.get("error", "Job submission failed"))

        if params.get("feed_name") or params.get("feed_id"):
            from airflow_plugins.dag_task_definitions.feed_control_callbacks import (
                run_post_submit_feed_control_or_fail,
            )
            run_post_submit_feed_control_or_fail(context)

        return result

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_pipelines_pharmacy_ods_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=303/pipeline/pipeline_id=1141/pharmacy_ods.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 652,
        "flow_id": 626,
        "pipeline_id": 1141,
        "feed_name": "sample_test7799",
        "validate_inbound_task_id": "validate_inbound_files",
        "facts_source": "databricks",
        "pipeline_name": "pharmacy_ods",
        "compute_xcom_key": "return_value",
        "valid_files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}",
        "batch_id": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='batch_id') }}"
    }
    run_pipelines_pharmacy_ods = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_pipelines_pharmacy_ods',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
        on_success_callback=feed_control_callbacks.submit_job_success_callback,
        on_failure_callback=feed_control_callbacks.submit_job_failure_callback,
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

        input_files = params.get("files")
        if isinstance(input_files, str) and "{" in input_files:
            input_files = context["task"].render_template(input_files, context)
            if isinstance(input_files, str):
                import ast
                try:
                    input_files = ast.literal_eval(input_files)
                except (ValueError, SyntaxError):
                    logger.warning("Rendered input_files is not valid Python literal", extra={"rendered_input_files": input_files})
                    input_files = None
        if input_files is None:
            input_files = []
        elif not isinstance(input_files, list):
            input_files = [input_files] if input_files else []
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

        def _is_truthy_folder_flag(value):
            return str(value).strip().lower() in {"true", "1", "yes"}

        def _is_directory_like_object(obj):
            key = str((obj or {}).get("key") or "")
            if not key or key.endswith("/"):
                return True
            metadata = (obj or {}).get("metadata") or {}
            if isinstance(metadata, dict) and _is_truthy_folder_flag(metadata.get("hdi_isfolder")):
                return True
            return False

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
            objects_with_metadata = []
            if hasattr(storage, "list_objects_with_metadata"):
                objects_with_metadata = storage.list_objects_with_metadata(
                    bucket_name=bucket_name,
                    prefix=source_prefix,
                ) or []

            if objects_with_metadata:
                files_to_archive = [
                    str(obj.get("key"))
                    for obj in objects_with_metadata
                    if not _is_directory_like_object(obj)
                ]
            else:
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
        "source_prefix": "",
        "archive_prefix": "Archive/testing",
        "cloud_type": "databricks",
        "airflow_connection_id": "databricks_default",
        "secret_name": "bh-dev-westus3-kv-key-scope/bh-azureblob-azureblobconn",
        "delete_source": True,
        "allow_empty": True,
        "files": "{{ task_instance.xcom_pull(task_ids='validate_inbound_files', key='valid_files') }}"
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
        user_account = conn.login
        if not user_account:
            try:
                import requests as _bh_rq
                _bh_me = _bh_rq.get(
                    workspace_url + '/api/2.0/preview/scim/v2/Me',
                    headers={'Authorization': 'Bearer ' + token},
                    timeout=10,
                )
                if _bh_me.status_code == 200:
                    _bh_d = _bh_me.json()
                    user_account = _bh_d.get('userName') or (_bh_d.get('emails') or [{}])[0].get('value')
            except Exception:
                pass
        user_account = user_account or 'unknown'
        if not workspace_url or not token:
            raise ValueError("Databricks connection must have host and password (token)")

        ti.xcom_push(key="bh_audit_metadata", value={
            "databricks_cluster_id": compute_id,
            "databricks_user_account": user_account,
            "ingestion_group_id": 652,
            "flow_id": 626
        })

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
    create_compute >> run_pipelines_employees_652
    run_pipelines_employees_652 >> run_pipelines_silver_raw_employees_to_target_employees_map_260701_9f15
    create_compute >> run_pipelines_silver_raw_employees_to_target_employees_map_260701_9f15
    run_pipelines_silver_raw_employees_to_target_employees_map_260701_9f15 >> run_pipelines_pharmacy_ods
    create_compute >> run_pipelines_pharmacy_ods
    run_pipelines_pharmacy_ods >> archive_processed_files
    archive_processed_files >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
