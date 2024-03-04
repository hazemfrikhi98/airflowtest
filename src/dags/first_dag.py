import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


my_dag = DAG(
    dag_id='first_dag',
    description='first_dag',
    tags=['poc',],
    schedule_interval='* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    },
    catchup=False
)


def print_date_and_hello():
    print(datetime.datetime.now())
    print('Hello caas cnp')


my_task = PythonOperator(
    task_id='first_dag',
    python_callable=print_date_and_hello,
    dag=my_dag
)
