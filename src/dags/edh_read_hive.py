import os
import pandas as pd
import turbodbc
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

my_dag = DAG(
    dag_id='edh_read_hive',
    description='edh_read_hive',
    tags=['poc', ],
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    },
    catchup=False
)

def read_from_edh_hive():
    EDH_USER = "fpvz2841"
    EDH_PASSWORD = "Hf11061998_"
    DSN = "EDHv2EXPLO"
    print(os.environ)

    params = {"SSP_tez.queue.name": "COSY"}
    options = turbodbc.make_options(
        read_buffer_size=turbodbc.Megabytes(int(os.getenv("EDH_BUFFERSIZE_MB", 90))),
        varchar_max_character_limit=10000,
        use_async_io=True,
        prefer_unicode=True,
        autocommit=True,
        large_decimals_as_64_bit_types=True,
        limit_varchar_results_to_max=True,
    )

    connection = turbodbc.connect(
        dsn=DSN,
        uid=EDH_USER,
        pwd=EDH_PASSWORD,
        turbodbc_options=options
    )
    cursor = connection.cursor()
    db = "uat_usg_cosy"
    table = f"{db}.watch_metrics_prepared"
    rows = pd.DataFrame()
    sql_peak = f""" select * from {table} where ci_name in ("MMA-1602P1:EFM0:1","MMA-5928P11:A0:35") and month="01" and year="2024" """
    cursor.execute(sql_peak)
    all_data = cursor.fetchallarrow()
    data_init = all_data.to_pandas(deduplicate_objects=True)
    print(data_init.head())
    print(data_init.info())

my_task = PythonOperator(
    task_id='edh_read_webhdfs',
    python_callable=read_from_edh_hive,
    dag=my_dag
)
