
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.dag_task_definitions.common_task import CommonTask
from airflow_plugins.dag_task_definitions.lineage_task import LineageTask

common_task = CommonTask(dag_id='ingestion_m1', dag_params={})
lineage_task = LineageTask(dag_id='ingestion_m1', dag_params={})

default_args = {
    'owner': 'bh',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 0
}

with DAG(
    dag_id='ingestion_m1',
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
            'flow_id': 476,
            'flow_name': 'ingestion_m1',
            'flow_key': 'ingestion_m1',
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

    def create_databricks_cluster_create_compute_9004b6341(**context):
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
                "cluster_name": "Databricks_AK_Local_0303_V1",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "Standard_D4ds_v5",
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
                    "/Workspace/Shared/bh-dev-utils/scripts/bh_test_databricks_grpc_server_codeartifact.sh"
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
        return cluster_id

    create_compute_9004b6341 = PythonOperator(
        task_id='create_compute_9004b6341',
        python_callable=create_databricks_cluster_create_compute_9004b6341,
        on_success_callback=common_task.success_callback,
        on_failure_callback=common_task.failure_callback,
    )


    from airflow.operators.python import PythonOperator
    end_flow_task = PythonOperator(
        task_id='end_flow_task',
        pre_execute=common_task.pre_execute_callback,
        python_callable=common_task.end_dag_task,
        on_success_callback=common_task.flow_success_callback,
        on_failure_callback=common_task.failure_callback,
    )

    start_flow_task >> create_compute_9004b6341
    create_compute_9004b6341 >> end_flow_task
