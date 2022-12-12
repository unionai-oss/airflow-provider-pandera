from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pandas import DataFrame
from pandera import Column

from pandera_provider.operators import PanderaOperator


def generate_dataframe(*args, **kwargs):
    ti = kwargs["ti"]
    df = DataFrame({"column1": ["pandera", "is", "awesome"]})
    ti.xcom_push("dfs_operator_df", df)


@dag(
    dag_id="dataframe_schema_success_dag",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule="0 0 * * *",
)
def dataframe_schema_success_dag(*args, **kwargs):

    generate_dataframe_task = PythonOperator(
        task_id="generate_dataframe_task",
        python_callable=generate_dataframe,
    )

    validate_dataframe_task = PanderaOperator(
        task_id="validate_dataframe_task",
        columns={
            "column1": Column(str),
        },
    )

    generate_dataframe_task >> validate_dataframe_task


dataframe_schema_success_dag()
