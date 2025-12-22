
from airflow import DAG

import datetime
import pendulum 

from airflow.providers.standard.operators.bash import BashOperator
# from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG


with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )
    
    bash_t1 >> bash_t2
    
    