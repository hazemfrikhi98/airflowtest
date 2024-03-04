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
    import io
    import pandas as pd

    EDH_USER = "fpvz2841"
    EDH_PASSWORD = "Hf11061998_"
    print(os.environ)

    import subprocess
    output = subprocess.check_output(["pip", "freeze"])
    print(output.decode())

    # to list all files :
    # https://edhu-portal.nor.fr.gin.intraorange/gateway/default/webhdfs/v1/UAT/use_case/COSY/CAPACITY_PLANNING/FEATURES/customer=laposte?op=LISTSTATUS

    base_url = "https://edhu-portal.nor.fr.gin.intraorange:443/gateway/default/webhdfs/v1/"
    hdfs_path = ("UAT/use_case/COSY/CAPACITY_PLANNING/FEATURES/customer=laposte/part-00044-a3517709-8b95-4e5a-9bb8-897395755de1-c000.snappy.orc")
    operation = "?op=OPEN"
    url_file = urljoin(base_url, hdfs_path + operation)

    script_abs_path = os.path.dirname(os.path.realpath(__file__))
    local_filename = script_abs_path + '/../output/' + hdfs_path.split('/')[-1]

    with requests.get(url_file, stream=True, auth=(EDH_USER, EDH_PASSWORD), verify=False) as r:
        r.raise_for_status()
        print(f'reading: {url_file}')

        # # write to local file
        # print(f'writing to: {local_filename}')
        # with open(local_filename, 'wb') as f:
        #     for chunk in r.iter_content(chunk_size=8192):
        #         f.write(chunk)
        # print('done')

        # Create a buffer to hold the ORC file contents
        buffer = io.BytesIO()
        for chunk in r.iter_content(chunk_size=8192):
            buffer.write(chunk)

        # Rewind the buffer to the beginning
        buffer.seek(0)

        # Read the ORC file directly from the buffer into a Pandas DataFrame
        df = pd.read_orc(buffer)

        # import pyarrow.orc as orc
        # data = orc.ORCFile(local_filename)
        # df = data.to_pandas()

        print(df.head())
        print(df.info())


my_task = PythonOperator(
    task_id='edh_read_webhdfs',
    python_callable=read_from_edh,
    dag=my_dag
)


# test read_from_edh locally
if __name__ == "__main__":
    read_from_edh()
