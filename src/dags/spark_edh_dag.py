from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id='edh_spark',
    description='edh_spark',
    tags=['poc',],
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    },
    catchup=False
)

submit_job = SparkSubmitOperator(
    task_id='submit_job',
    application='spark_job.py',
    conn_id='spark_default',
    dag=dag,
)
