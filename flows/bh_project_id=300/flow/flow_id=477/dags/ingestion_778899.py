
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='ingestion_7171', dag_params={})
lineage_task = LineageTask(dag_id='ingestion_7171', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='ingestion_7171',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=[]
) as dag:


    from airflow.operators.python import PythonOperator
    start_flow_task = PythonOperator(
        task_id='start_flow_task',
        python_callable=common_task.start_dag_task,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
        params = {
            'flow_id': 477,
            'flow_name': 'ingestion_7171',
            'flow_key': 'ingestion_7171',
            'bh_project_id': 300,
            'project_name': 'Ingestion_1',
            'flow_tags': [],
            'flow_type': 'INGESTION',
            'tenant_id': 20,
            'flow_status': 'In Progress',
        }
    )

    from airflow.operators.python import PythonOperator
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    def create_databricks_cluster_create_compute_0fa4f10c4(**context):
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
                "cluster_name": "Verato999",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v3",
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
                "autotermination_minutes": None,
                "enable_elastic_disk": True,
                "spark_conf": {},
                "spark_env_vars": {
                    "VERATO_SECRET_SCOPE": "bh_secret_scope_verato",
                    "VERATO_USERNAME_KEY": "VERATO_USERNAME",
                    "VERATO_PASSWORD_KEY": "VERATO_PASSWORD",
                    "VERATO_INPUT_CATALOG": "enrollment",
                    "VERATO_INPUT_SCHEMA": "silverraw",
                    "VERATO_BATCH_ID": 1,
                    "VERATO_SOURCE_SYSTEM": "centene_ga_medicaid",
                    "VERATO_OUTPUT_TABLE": "enrollment.silverraw.verato_post_identity_audit_new777",
                    "SECRET_MANAGER_PROVIDER": "databricks"
                },
                "custom_tags": {},
                "init_scripts": [
                    ""
                ],
                "libraries": [],
                "databricks_region": "us-west-1",
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
        return cluster_id

    create_compute_0fa4f10c4 = PythonOperator(
        task_id='create_compute_0fa4f10c4',
        python_callable=create_databricks_cluster_create_compute_0fa4f10c4,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    from bh_logger import get_logger
    logger = get_logger(__name__)

    def submit_notebook_to_cluster(**context):
        params = context.get("params") or {}
        job_config = params.get("job_config")
        if not job_config:
            raise ValueError("Missing job_config in params")

        # Prefer compute_id from params (supports Jinja xcom_pull strings), fallback to XCom.
        compute_id = params.get("compute_id")
        xcom_key = str(params.get("compute_xcom_key") or "return_value")
        if not compute_id or (isinstance(compute_id, str) and "{{" in compute_id):
            ti = context["ti"]
            # Most flows normalize the create task_id to 'create_compute'. Keep a legacy fallback.
            compute_task_id = params.get("compute_task_id") or "create_compute"
            compute_id = ti.xcom_pull(task_ids=compute_task_id, key=xcom_key)
            if not compute_id:
                compute_id = ti.xcom_pull(task_ids="databricks_create_cluster_task", key=xcom_key)

        if not compute_id or (isinstance(compute_id, str) and "{{" in compute_id):
            raise ValueError("No compute_id from params or XCom")

        # Resolve connection: User UI entry -> upstream CreateCompute XCom -> fallback 'databricks_default'
        airflow_connection_id = params.get("airflow_connection_id")
        if not airflow_connection_id or (isinstance(airflow_connection_id, str) and "{{" in airflow_connection_id):
            ti = context["ti"]
            compute_task_id = params.get("compute_task_id") or "create_compute"
            airflow_connection_id = ti.xcom_pull(task_ids=compute_task_id, key="airflow_connection_id")

        if not airflow_connection_id or (isinstance(airflow_connection_id, str) and "{{" in airflow_connection_id):
            airflow_connection_id = "databricks_default"

        from airflow.hooks.base import BaseHook
        try:
            conn = BaseHook.get_connection(airflow_connection_id)
            workspace_url = (conn.host or '').rstrip('/')
            token = conn.password
            if not workspace_url or not token:
                raise ValueError(f"Databricks connection '{airflow_connection_id}' must have host and password (token)")
        except Exception as e:
            logger.exception("Failed to retrieve Airflow connection '%s'", airflow_connection_id)
            raise RuntimeError(f"Airflow connection lookup failed for '{airflow_connection_id}': {str(e)}") from e

        try:
            factory = CloudFactory("databricks", databricks_workspace_url=workspace_url, databricks_token=token)
            compute = factory.get_compute(compute_type="databricks")
            result = compute.execute_job(compute_id, job_config, run_async=False)
            if result.get("status") == "FAILED":
                error_details = result.get("error", "Unknown Databricks error")
                raise RuntimeError(f"Databricks notebook execution failed: {error_details}")
            run_id = result.get("run_id")
            if run_id:
                context["ti"].xcom_push(key="run_id", value=run_id)
            return result
        except Exception as e:
            logger.exception(
                "Exception occurred during Databricks notebook execution. "
                "Notebook Path: %s, Cluster ID: %s, Workspace: %s",
                job_config.get("notebook_path"), compute_id, workspace_url
            )
            raise RuntimeError(f"Failed to execute Databricks notebook: {str(e)}") from e

    _submit_notebook_params = {
        "compute_task_id": "create_compute_0fa4f10c4",
        "job_config": {
            "job_type": "notebook",
            "notebook_path": "/Workspace/Users/sasikumar@bighammer.ai/verato_7",
            "parameters": {}
        },
        "airflow_connection_id": "databricks_default",
        "compute_xcom_key": "return_value"
    }
    submit_notebook_0964b6b91 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='submit_notebook_0964b6b91',
        python_callable=submit_notebook_to_cluster,
        params=_submit_notebook_params,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    from airflow.operators.python import PythonOperator
    from airflow_plugins.cloud_factory import CloudFactory
    import logging
    logger = logging.getLogger(__name__)

    def terminate_databricks_resources(**context):
        ti = context["ti"]
        compute_id = ti.xcom_pull(task_ids="create_compute_0fa4f10c4", key="return_value")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            params = context.get("params") or {}
            compute_id = params.get("compute_id")
        if not compute_id or (isinstance(compute_id, str) and "{" in compute_id):
            logger.warning("No compute_id from XCom task create_compute_0fa4f10c4 or params; skipping terminate")
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
    delete_compute_8ce4ed311 = PythonOperator(
        pre_execute=common_task.pre_execute_callback,
        task_id='delete_compute_8ce4ed311',
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

    start_flow_task >> create_compute_0fa4f10c4
    create_compute_0fa4f10c4 >> submit_notebook_0964b6b91
    submit_notebook_0964b6b91 >> delete_compute_8ce4ed311
    create_compute_0fa4f10c4 >> delete_compute_8ce4ed311
    delete_compute_8ce4ed311 >> end_flow_task
