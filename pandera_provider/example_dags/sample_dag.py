from datetime import datetime
from airflow import DAG
from pandera_provider.operators import DataFrameSchemaOperator

default_dag_args = {
        "dag_id": "pandera_provider_dag",
        "start_date": datetime(2022, 1, 1)
        }

with DAG(**default_dag_args, schedule="@daily") as dag:
    task1 = DataFrameSchemaOperator(task_id="task1")
    task2 = DataFrameSchemaOperator(task_id="task2")

    task1 >> task2
