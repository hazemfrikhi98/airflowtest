import os
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


dag = DAG(
    dag_id="spark_job_example",
    description='spark_job_example',
    tags=['poc',],
    schedule_interval="@daily",
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    },
    catchup=False
)


def print_debug():
    print('############')
    print(datetime.datetime.now())
    print(os.environ)
    print(os.path.dirname(os.path.realpath(__file__)))
    print('############')


debug_task = PythonOperator(
    task_id='debug_task',
    python_callable=print_debug,
    dag=dag
)

submit_job = SparkSubmitOperator(
    task_id="submit_spark_job",
    conn_id='spark_default',
    application="./spark_job.py",
    name="my-spark-app", 
    dag=dag,
)

debug_task >> submit_job
