from pathlib import Path
from tempfile import gettempdir

import pendulum
import pytest
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pandas import DataFrame
from pandera import Column, DataFrameSchema, SchemaModel
from pandera.typing import Series

from pandera_provider.operators.pandera import PanderaOperator


@pytest.fixture
def simple_df():
    return DataFrame(
        {
            "column1": ["pandera", "is", "awesome"],
            "column2": [1, 2, 3],
            "column3": [0.1, 0.2, 0.3],
        }
    )


@pytest.fixture
def wrong_schema_df():
    return DataFrame(
        {
            "column1": ["pandera", "is", "awesome"],
            "column2": [1, 2, 3],
            "column3": [
                "should",
                "be",
                "floats",
            ],  # tests are going to check for a float type here
        }
    )


@pytest.fixture
def dataframeschema():
    return DataFrameSchema(
        columns={
            "column1": Column(str),
            "column2": Column(int),
            "column3": Column(float),
        }
    )


@pytest.fixture
def schemamodel():
    class Schema(SchemaModel):
        column1: Series[str]
        column2: Series[int]
        column3: Series[float]

    return Schema


@pytest.fixture
def dataframeschema_csv_simple_df_dag(simple_df, dataframeschema):
    tmpdir = gettempdir()
    tmp_csv_file = Path(tmpdir, "test.csv")

    def csv_generator_callable():
        simple_df.to_csv(tmp_csv_file, index=False)

    @dag(
        dag_id="dataframeschema_csv_simple_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="csv_generator_task", python_callable=csv_generator_callable
        )

        validate_dataframe_task = PanderaOperator(
            filepath=tmp_csv_file,
            task_id="validate_dataframe_task",
            dataframeschema=dataframeschema,
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def dataframeschema_csv_empty_df_dag(dataframeschema):
    tmpdir = gettempdir()
    tmp_csv_file = Path(tmpdir, "test.csv")

    def csv_generator_callable():
        DataFrame().to_csv(tmp_csv_file, index=False)

    @dag(
        dag_id="dataframeschema_csv_empty_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="csv_generator_task", python_callable=csv_generator_callable
        )

        validate_dataframe_task = PanderaOperator(
            filepath=tmp_csv_file,
            task_id="validate_dataframe_task",
            dataframeschema=dataframeschema,
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def dataframeschema_simple_df_dag(simple_df, dataframeschema):
    @dag(
        dag_id="dataframeschema_simple_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=simple_df
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=dataframeschema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def dataframeschema_wrong_xcom_key_dag(simple_df, dataframeschema):
    @dag(
        dag_id="dataframeschema_wrong_xcom_key_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="wrong_key", value=simple_df
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=dataframeschema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def dataframeschema_empty_df_dag(dataframeschema):
    @dag(
        dag_id="dataframeschema_empty_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=DataFrame()
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=dataframeschema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def dataframeschema_wrong_schema_df_dag(wrong_schema_df, dataframeschema):
    @dag(
        dag_id="dataframeschema_wrong_schema_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=wrong_schema_df
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=dataframeschema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_csv_simple_df_dag(simple_df, schemamodel):
    tmpdir = gettempdir()
    tmp_csv_file = Path(tmpdir, "test.csv")

    def csv_generator_callable():
        simple_df.to_csv(tmp_csv_file, index=False)

    @dag(
        dag_id="schemamodel_csv_simple_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="csv_generator_task", python_callable=csv_generator_callable
        )

        validate_dataframe_task = PanderaOperator(
            filepath=tmp_csv_file,
            task_id="validate_dataframe_task",
            dataframeschema=schemamodel,
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_csv_empty_df_dag(schemamodel):
    tmpdir = gettempdir()
    tmp_csv_file = Path(tmpdir, "test.csv")

    def csv_generator_callable():
        DataFrame().to_csv(tmp_csv_file, index=False)

    @dag(
        dag_id="schemamodel_csv_empty_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="csv_generator_task", python_callable=csv_generator_callable
        )

        validate_dataframe_task = PanderaOperator(
            filepath=tmp_csv_file,
            task_id="validate_dataframe_task",
            dataframeschema=schemamodel,
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_simple_df_dag(simple_df, schemamodel):
    @dag(
        dag_id="schemamodel_simple_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=simple_df
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=schemamodel
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_wrong_xcom_key_dag(simple_df, dataframeschema):
    @dag(
        dag_id="schemamodel_wrong_xcom_key_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="wrong_key", value=simple_df
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=dataframeschema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_empty_df_dag(schemamodel):
    @dag(
        dag_id="schemamodel_empty_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=DataFrame()
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=schemamodel
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_wrong_schema_df_dag(wrong_schema_df, schemamodel):
    @dag(
        dag_id="schemamodel_wrong_schema_df_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=wrong_schema_df
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", dataframeschema=schemamodel
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()
