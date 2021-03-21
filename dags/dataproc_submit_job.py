from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryInsertJobOperator
    )
from airflow.providers.google.cloud.operators.dataproc import (
        DataprocSubmitJobOperator
    )
from airflow.models.variable import Variable
from airflow import DAG
from datetime import datetime, timedelta
from package.api.google.cloud.dataproc import DataprocCreateClusterConfig
import pendulum


local_tz = pendulum.timezone("Asia/Taipei")
gcp_config = Variable.get("gcp_project_1", deserialize_json=True)
dataproc_config = gcp_config["dataproc"]
bucket_config = dataproc_config["bucket"]
cluster_config = DataprocCreateClusterConfig.make(gcp_config)
bigquery_load_storage_config={
        "load": {
            "source_uris": [f"gs://{bucket_config['data_lake']}/'your-path'/upload_date={{ ds }}/*.parquet"],
            "source_format": "PARQUET", 
            "destination_table": {
                "project_id": gcp_config["project_id"],
                "dataset_id": "YOUR-DATASET",
                "table_id": "YOUR-TABLE"
                },
            "create_disposition": "CREATE_IF_NEEDED", 
            "write_disposition": "WRITE_APPEND",
            "time_partitioning": {
                "type":"DAY",
                "field":"event_time"
                }, 
            "clustering": {
                "fields": ["YOUR_COLUMNS"]
                },
            }
        }


class MyDataprocSubmitJobOperator(DataprocSubmitJobOperator):
    template_fields = ('job',)


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2021, 3, 4, tzinfo=local_tz),
    'email': ['jw840908@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1), 
    'project_id': gcp_config['project_id'], 
    'region': gcp_config['region'], 
    'location': gcp_config['region'], 
    'gcp_conn_id': gcp_config['conn_id']
}


with DAG(
    'dataproc_submit_job', 
    default_args=default_args,
    description='dataproc_submit_job',
    schedule_interval='@once'
    ) as dag:


    logs_to_parquet = MyDataprocSubmitJobOperator(
        task_id='logs_to_parquet',
        job={
            'placement': {
                'cluster_name': dataproc_config['cluster_name']
            },
            'pyspark_job': {
                'main_python_file_uri': f"gs://{bucket_config['jobs']}/jobs/UploadFilesToParquet.py", 
                'properties': {
                    'spark.master': 'yarn', 
                    'spark.deploy-mode': 'cluster', 
                    'spark.driver.cores': '1', 
                    'spark.driver.memory': '1g', 
                    'spark.executor.cores': '2', 
                    'spark.executor.memory': '4g', 
                    'spark.executor.instances': '4'
                }, 
                'args': [
                    '--GCS_UPLOAD_URI', f"gs://{bucket_config['upload_data']}/logs/20210101/*.log", 
                    '--GCS_DATA_LAKE_URI', '/user/hdfs/raw_data/'
                ]
            } 
        }
    )

    hdfs_to_bigquery = BigQueryInsertJobOperator(
            task_id='hdfs_to_bigquery', 
            configuration=bigquery_load_storage_config
        )


logs_to_parquet >> hdfs_to_bigquery