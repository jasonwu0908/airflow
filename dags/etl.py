from airflow.providers.google.cloud.operators.gcs import (
        GCSListObjectsOperator
        )
from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryInsertJobOperator
        )
from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateClusterOperator, 
        DataprocDeleteClusterOperator, 
        DataprocSubmitJobOperator
        )
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.variable import Variable
from airflow import DAG
from package.api.google.cloud.dataproc import DataprocCreateClusterConfig
from datetime import datetime, timedelta
import pendulum


local_tz = pendulum.timezone("Asia/Taipei")
gcp_config = Variable.get("gcp_project_1", deserialize_json=True)
dataproc_config = gcp_config["dataproc"]
bucket_config = dataproc_config["bucket"]
cluster_config = DataprocCreateClusterConfig.make(gcp_config)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2021, 3, 7, tzinfo=local_tz),
    'email': ['jw840908@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1), 
    'postgres_conn_id': 'YOUR_POSTGRES_CONN_ID', 
    'gcp_conn_id': gcp_config['conn_id'], 
    'database': 'YOUR_DB'
}

def _check_if_log_exists(ti):
    logs = ti.xcom_pull(task_ids=['check_gcs_logs'], key="return_value")[0]
    if logs:
        # return ['create_dataproc', 'initial_ato_sn_calc_final', 'initial_ato_sn_calc_daily']
        return ['initial_ato_sn_calc_final', 'initial_ato_sn_calc_daily']

    return 'branch_b'


with DAG(
    'etl', 
    default_args=default_args,
    description='etl',
    schedule_interval='36 17 * * *'
    ) as dag:

    check_gcs_logs = GCSListObjectsOperator(
        task_id='check_gcs_logs',
        bucket=bucket_config['upload_data'], 
        prefix='logs/{{ ds_nodash }}', 
        delimiter='.log', 
        gcp_conn_id=gcp_config['conn_id']
    )


    branching = BranchPythonOperator(
        task_id='branching', 
        python_callable=_check_if_log_exists
    )    
    

    branch_b = DummyOperator(
        task_id='branch_b'
    )

    # create_dataproc = DataprocCreateClusterOperator(
    #     task_id="create_dataproc",
    #     project_id=gcp_config["project_id"], 
    #     cluster_name=dataproc_config["cluster_name"], 
    #     region=gcp_config["region"], 
    #     cluster_config=cluster_config, 
    #     gcp_conn_id=gcp_config["conn_id"]
    # )

    # delete_dataproc = DataprocDeleteClusterOperator(
    #     task_id="delete_dataproc",
    #     project_id=gcp_config["project_id"], 
    #     cluster_name=dataproc_config["cluster_name"], 
    #     region=gcp_config["region"], 
    #     gcp_conn_id=gcp_config["conn_id"]
    # )


    initial_user_final_calc = PostgresOperator(
        task_id='initial_user_final_calc', 
        sql="""
            CREATE TABLE IF NOT EXISTS user_final_calc ( 
                user_id                  VARCHAR(20)  PRIMARY KEY,
                ...
                update_time              TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP
        )"""
    )

    initial_user_daily_calc = PostgresOperator(
        task_id='initial_user_daily_calc', 
        sql="""
            CREATE TABLE IF NOT EXISTS user_daily_calc ( 
                id                       SERIAL       PRIMARY KEY,
                user_id                  VARCHAR(20)  NOT NULL,
                event_date               DATE         NOT NULL, 
                ...
                upload_date              DATE         NOT NULL, 
                update_time              TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP
        )"""
    )

    drop_user_daily_calc_temp = PostgresOperator(
        task_id='drop_user_daily_calc_temp', 
        sql="""
            DROP TABLE IF EXISTS user_daily_calc_temp
            """
    )

    create_user_daily_calc_temp = PostgresOperator(
        task_id='create_user_daily_calc_temp', 
        sql="""
            CREATE TABLE IF NOT EXISTS user_daily_calc_temp AS (
                SELECT
                    user_id, 
                    DATE(start_time) AS event_date, 
                    ...
                    DATE(upload_time) AS upload_date, 
                    CURRENT_TIMESTAMP AS update_time
                FROM YOUR_TABLE
                WHERE upload_time BETWEEN '{{ ds }}' AND '{{ tomorrow-ds }}'
                GROUP BY user_id, DATE(start_time), DATE(upload_time)
            )
            """
    )

    drop_user_final_calc_temp = PostgresOperator(
        task_id='drop_user_final_calc_temp', 
        sql="""
            DROP TABLE IF EXISTS user_final_calc_temp
            """
    )


    create_user_final_calc_temp = PostgresOperator(
        task_id='create_user_final_calc_temp', 
        sql="""
            CREATE TABLE IF NOT EXISTS user_final_calc_temp AS (
                SELECT 
                    a.user_id, 
                    ...
                    MAX(a.update_time) AS update_time
                FROM
                (
                    (
                        SELECT
                            *
                        FROM user_final_calc
                    )
                    UNION
                    (
                        SELECT
                            user_id, 
                            ...
                            update_time
                        FROM user_daily_calc_temp
                    )
                ) AS a
                GROUP BY a.user_id
            )
            """
    )

    update_user_final_calc = PostgresOperator(
        task_id='update_user_final_calc', 
        sql="""
            UPDATE 
                user_final_calc
            SET 
                ... = a. ... 
                update_time = CURRENT_TIMESTAMP
            FROM 
                user_final_calc_temp AS a
            WHERE
                user_final_calc.user_id = a.usr_id 
            """
    )

    insert_user_final_calc = PostgresOperator(
        task_id='insert_user_final_calc', 
        sql="""
            INSERT INTO user_final_calc (
                SELECT 
                    user_id, 
                    ...
                    update_time
                FROM user_final_calc_temp
                WHERE user_id NOT IN (
                    SELECT 
                        a.user_id   
                    FROM 
                        user_final_calc a
                    LEFT JOIN 
                        user_final_calc_temp b
                    ON
                        a.user_id=b.user_id
                )
            )
            """
    )


    insert_user_daily_calc = PostgresOperator(
        task_id='insert_user_daily_calc', 
        sql="""
            INSERT INTO user_daily_calc (
                user_id, 
                event_date, 
                ...
                upload_date
            )(
                SELECT 
                    user_id, 
                    event_date,
                    ...
                    upload_date
                FROM user_daily_calc_temp
            )
            """
    )


check_gcs_logs >> branching >> [initial_user_final_calc, initial_user_daily_calc, branch_b]
[initial_user_final_calc, initial_user_daily_calc] >> drop_user_daily_calc_temp >> create_user_daily_calc_temp >> [insert_user_daily_calc, drop_user_final_calc_temp]
drop_user_final_calc_temp >> create_user_final_calc_temp >> update_user_final_calc >> insert_user_final_calc