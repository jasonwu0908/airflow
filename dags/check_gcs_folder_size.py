from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from google.cloud import storage
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email': ['jw840908@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def get_folder_size(**kwargs):
    client = storage.Client()
    all_blobs = list(client.list_blobs('YOUR-BUCKET-NAME', prefix='YOUR-PATH'))
    folder_size = sum(map(lambda x: x.size, all_blobs))
    if folder_size > 10000000:
        num_instances = 4
        kwargs['ti'].xcom_push(key='gcs', value=num_instances)
    else:
        num_instances = 2
        kwargs['ti'].xcom_push(key='gcs', value=num_instances)


with DAG(
    'check_gcs_folder_size',
    default_args=default_args,
    description='check_gcs_folder_size',
    schedule_interval='@once'
    ) as dag:

    get_folder_size = PythonOperator(
        task_id='get_folder_size',
        python_callable=get_folder_size
    )

    show_xcom = BashOperator(
        task_id="show_xcom",
        bash_command='echo "{{ ti.xcom_pull(key="gcs") }}"'
    )


get_folder_size >> show_xcom
