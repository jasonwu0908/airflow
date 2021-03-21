from airflow.providers.google.cloud.operators.gcs import (
        GCSListObjectsOperator
        )
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email': ['jw840908@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1), 
    'gcp_conn_id': 'gcp_project_1'
}


def _check_if_log_exists(ti):
    logs = ti.xcom_pull(task_ids=['get_gcs_object_list'], key="return_value")[0]
    if logs:
        return 'branch_a'
    return 'branch_b'


with DAG(
    'tacc_branch_test', 
    default_args=default_args,
    description='Test task: branch',
    schedule_interval='@once'
    ) as dag:


    get_gcs_object_list = GCSListObjectsOperator(
        task_id='get_gcs_object_list',
        bucket='your-bucket-name', 
        prefix='your-path', 
        delimiter='.log'
    )

    branching = BranchPythonOperator(
        task_id='branching', 
        python_callable=_check_if_log_exists
    )

    branch_a = DummyOperator(
        task_id='branch_a'
    )

    branch_b = DummyOperator(
        task_id='branch_b'
    )


get_gcs_object_list >> branching >> [branch_a, branch_b]