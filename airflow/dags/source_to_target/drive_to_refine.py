import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
    dag_id="drive_to_refine",
    default_args=default_args,
    start_date=pendulum.datetime(year=2025, month=12, day=1, tz="Asia/Jakarta"),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["drive", "refine"],
)

mplus_stt = BashOperator(
    task_id="mplus_stt",
    depends_on_past=False,
    bash_command=(
        "cd /script/staging ; "
        "python mplus_stt.py || exit 3"
    ),
    dag=dag,
)

ref_mplus_stt = BashOperator(
    task_id="ref_mplus_stt",
    bash_command=(
        "cd /script/refine ; "
        "python ref_mplus_stt.py || exit 3"
    ),
    dag=dag,
)

mplus_stt >> ref_mplus_stt
