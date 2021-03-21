from airflow.providers.google.cloud.operators.dataproc import (
        DataprocDeleteClusterOperator
    )
from airflow.models.variable import Variable
from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from package.api.google.cloud.dataproc import DataprocCreateClusterConfig


local_tz = pendulum.timezone('Asia/Taipei')
gcp_config = Variable.get('gcp_project_1', deserialize_json=True)
dataproc_config = gcp_config['dataproc']
bucket_config = dataproc_config['bucket']
cluster_config = DataprocCreateClusterConfig.make(gcp_config)



default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2021, 3, 9, tzinfo=local_tz),
    'email': ['jw840908@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1), 
    'project_id': gcp_config['project_id'], 
    'region': gcp_config['region'], 
    'gcp_conn_id': gcp_config['conn_id']
}


with DAG(
    'delete_dataproc', 
    default_args=default_args,
    description='delete_dataproc',
    schedule_interval='@once'
    ) as dag:


    delete_dataproc = DataprocDeleteClusterOperator(
        task_id='delete_dataproc',
        cluster_name=dataproc_config['cluster_name']
    )

delete_dataproc
