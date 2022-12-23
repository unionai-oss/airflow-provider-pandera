from datetime import datetime
from pathlib import Path
from tempfile import gettempdir

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pandas import DataFrame
from pandera import Column, DataFrameSchema

from pandera_provider.operators.pandera import PanderaOperator

TMPDIR = gettempdir()
TMPFILE = Path(TMPDIR, "test.csv")


def generate_dataframe():
    df = DataFrame(
        {
            "column1": ["pandera", "is", "awesome"],
            "column2": [1, 2, 3],
            "column3": [0.1, 0.2, 0.3],
        }
    )
    df.to_csv(TMPFILE, index=False)


@dag(
    dag_id="dataframeschema_csv_dag",
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
        filepath=TMPFILE,
        task_id="validate_dataframe_task",
        dataframeschema=DataFrameSchema(
            columns={
                "column1": Column(str),
                "column2": Column(int),
                "column3": Column(float),
            }
        ),
    )

    generate_dataframe_task >> validate_dataframe_task


dataframe_schema_success_dag()
