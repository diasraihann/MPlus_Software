import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="dm_mplus_billing_dmart",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Jakarta"),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["mplus", "refine", "dmart"],
)

# Task : Running dmart
dm_mplus_billing = BashOperator(
    task_id="dm_mplus_billing",
    bash_command=(
        "cd /script/dmart ; "
        "python dm_mplus_billing.py || exit 3"
    ),
    dag=dag,
)
