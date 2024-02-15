from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


my_dag = DAG(
    dag_id='edh_read_webhdfs',
    description='edh_read_webhdfs',
    tags=['poc',],
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    },
    catchup=False
)


def read_from_edh():
    import os
    from urllib.parse import urljoin
    import requests

    EDH_USER = "nslt4939_muxmuxc"
    EDH_PASSWORD = os.getenv("DOCKER_PW")
    print(os.environ)

    base_url = "https://edhu-portal.nor.fr.gin.intraorange:443/gateway/default/webhdfs/v1/"
    hdfs_path = ("UAT/use_case/MUXC/WATCH_METRICS/year=2023/month=05/day=01/hour=00/minute=00/"
                 "part-00000-9f65511a-0813-4a01-91ea-10ffd7b40629-c000.snappy.orc")
    operation = "?op=OPEN"
    url_file = urljoin(base_url, hdfs_path + operation)

    local_filename = hdfs_path.split('/')[-1]
    with requests.get(url_file, stream=True, auth=(EDH_USER, EDH_PASSWORD), verify=False) as r:
        r.raise_for_status()
        print(f'reading: {url_file}')
        print(f'writing to: {local_filename}')
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    import pandas as pd
    from pyarrow import orc
    table = orc.read_table(local_filename)
    df = table.to_pandas()

    print(df.head())
    print(df.info())


my_task = PythonOperator(
    task_id='edh_read_webhdfs',
    python_callable=read_from_edh,
    dag=my_dag
)
