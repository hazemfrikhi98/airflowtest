from __future__ import annotations

import os
from datetime import datetime, timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# [END import_module]


# [START instantiate_dag]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "spark_pi"

@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    """Watcher task raises an AirflowException and is used to 'watch' tasks for failures
    and propagates fail status to the whole DAG Run"""
    raise AirflowException("Failing task because one or more upstream tasks failed.")

with DAG(
    DAG_ID,
    default_args={"max_active_runs": 1},
    description="submit watch spark-py as sparkApplication on kubernetes",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START SparkKubernetesOperator_DAG]
    t1 = SparkKubernetesOperator(
        task_id="spark_watch_py_submit",
        namespace="watch-spark-ai",
        application_file="spark-watch-py.yaml",
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(
        task_id="spark_watch_py_monitor",
        namespace="watch-spark-ai",
        application_name="{{ task_instance.xcom_pull(task_ids='spark_watch_py_submit')['metadata']['name'] }}",
        dag=dag,
    )
    t1 >> t2

    # [END SparkKubernetesOperator_DAG]

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

# from utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
# test_run = get_test_run(dag)