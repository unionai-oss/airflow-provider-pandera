from pathlib import Path
from pandas import DataFrame
from pandera_provider.operators import DataFrameSchemaOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_dag_args = {
    "dag_id": "pandera_provider_dag",
    "start_date": datetime(2022, 1, 1),
    "catchup": False,
}


def generate_dataframe():
    df = DataFrame({"column1": ["pandera", "is", "awesome"]})
    import pdb

    pdb.set_trace()
    return {"dataframe": df}


with DAG(**default_dag_args, schedule="@daily") as dag:

    generate_dataframe_task = PythonOperator(
        task_id="generate_dataframe_task", python_callable=generate_dataframe
    )

    validate_dataframe_task = DataFrameSchemaOperator(
        task_id="validate_dataframe_task", dataframe=DataFrame(), columns={}
    )

    generate_dataframe_task >> validate_dataframe_task
