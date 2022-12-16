import pendulum
import pytest
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pandas import DataFrame
from pandera import Column, DataFrameSchema, SchemaModel
from pandera.typing import Series

from pandera_provider.operators.pandera import PanderaOperator


@pytest.fixture
def dataframe():
    return DataFrame(
        {
            "column1": ["pandera", "is", "awesome"],
            "column2": [1, 2, 3],
            "column3": [0.1, 0.2, 0.3],
        }
    )


@pytest.fixture
def dataframeschema_success_dag(dataframe):
    @dag(
        dag_id="dataframe_schema_success_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=dataframe
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task",
            dataframeschema=DataFrameSchema(
                columns={
                    "column1": Column(str),
                    "column2": Column(int),
                    "column3": Column(float),
                }
            ),
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def dataframeschema_fail_dag(dataframe):
    @dag(
        dag_id="dataframeschema_fail_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_dataframeschema():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=dataframe
            ),
        )

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task",
            dataframeschema=DataFrameSchema(
                columns={
                    "column1": Column(str),
                    "column2": Column(str),
                    "column3": Column(float),
                }
            ),
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_dataframeschema()


@pytest.fixture
def schemamodel_success_dag(dataframe):
    @dag(
        dag_id="schema_model_success_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_schema_model():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=dataframe
            ),
        )

        class InputSchema(SchemaModel):
            column1: Series[str]
            column2: Series[int]
            column3: Series[float]

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", schema_model=InputSchema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_schema_model()


@pytest.fixture
def schemamodel_fail_dag(dataframe):
    @dag(
        dag_id="schemamodel_fail_dag",
        start_date=pendulum.datetime(2021, 9, 13, tz="UTC"),
        catchup=False,
        schedule="@daily",
    )
    def dag_test_schemamodel():
        df_generator_task = PythonOperator(
            task_id="df_generator_task",
            python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
                key="pandera_df", value=dataframe
            ),
        )

        class InputSchema(SchemaModel):
            column1: Series[str]
            column2: Series[float]
            column3: Series[float]

        validate_dataframe_task = PanderaOperator(
            task_id="validate_dataframe_task", schema_model=InputSchema
        )

        df_generator_task.set_downstream(validate_dataframe_task)

    return dag_test_schemamodel()
