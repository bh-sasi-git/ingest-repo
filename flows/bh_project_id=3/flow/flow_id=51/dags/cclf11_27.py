
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='cclf11_27', dag_params={})
lineage_task = LineageTask(dag_id='cclf11_27', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='cclf11_27',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['12346', 'dev']
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 51,
            'flow_name': 'CCLF11_27',
            'flow_key': 'cclf11_27',
            'bh_project_id': 3,
            'project_name': 'Ingest_repo1',
            'flow_tags': [{'key': 'drn', 'value': '12346'}, {'key': 'environment', 'value': 'dev'}],
            'flow_type': 'INGESTION',
            'tenant_id': 1,
            'flow_status': 'In Progress',
        }
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
                "cluster_name": "Databricks_1",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
                "num_workers": 0,
                "autoscale": None,
                "driver_node_type_id": None,
                "runtime_engine": None,
                "data_security_mode": "SINGLE_USER",
                "single_user_name": "sasikumar@bighammer.ai",
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
                    "SECRET_MANAGER_PROVIDER": "databricks"
                },
                "custom_tags": {},
                "init_scripts": [
                    "/Workspace/Shared/dev-utils/scripts/bh_databricks_grpc_server.sh"
                ],
                "libraries": [],
                "databricks_region": "us-west-2",
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

        num_workers = payload.get("num_workers", 0)
        context["ti"].xcom_push(key="bh_audit_metadata", value={
            "databricks_cluster_id": cluster_id,
            "databricks_cluster_size": num_workers,
            "databricks_user_account": user_account,
            "ingestion_group_id": 27,
            "flow_id": 51
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
                by_source[src_name].append(str(key).strip().lstrip("/"))
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
        return result

    _submit_params = {
        "compute_task_id": "create_compute",
        "job_config": {
            "job_type": "spark_python",
            "name": "{{ dag.dag_id }}_run_jobs_cclf11_{{ ts_nodash }}",
            "python_file": "/Workspace/Shared/dev-utils/pipelines/main.py",
            "parameters": [
                "/Workspace/Shared/codespace/pipelines/bh_project_id=3/pipeline/pipeline_id=12/cclf11.json",
                "databricks",
                "/Workspace/Shared/dev-utils/schemas"
            ]
        },
        "ingestion_group_id": 27,
        "flow_id": 51,
        "pipeline_id": 12,
        "compute_xcom_key": "return_value"
    }
    run_jobs_cclf11 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='run_jobs_cclf11',
        python_callable=submit_job_to_cluster,
        params=_submit_params,
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
            "ingestion_group_id": 27,
            "flow_id": 51
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

    start_flow_task >> create_compute
    create_compute >> run_jobs_cclf11
    run_jobs_cclf11 >> delete_compute
    create_compute >> delete_compute
    delete_compute >> end_flow_task
