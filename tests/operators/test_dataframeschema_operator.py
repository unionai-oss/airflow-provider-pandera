import pytest

from pandas import DataFrame
from pandera import Column
from pandera.errors import SchemaError
from pandera_provider.operators import DataFrameSchemaOperator


@pytest.fixture
def single_column_dataframe():

    df = DataFrame({"column1": ["pandera", "is", "awesome"]})
    return df


@pytest.fixture
def two_column_dataframe():

    df = DataFrame({"column1": ["pandera", "is", "awesome"], "column2": [1, 2, 3]})
    return df


class TestDataFrameSchemaOperatorClass:
    def test_operator_single_column_df_success(self, single_column_dataframe):

        columns = {
            "column1": Column(str),
        }

        operator = DataFrameSchemaOperator(
            task_id="test1", dataframe=single_column_dataframe, columns=columns
        )

        operator.execute()

    def test_operator_single_column_df_fail(self, single_column_dataframe):

        with pytest.raises(SchemaError):

            columns = {
                "column1": Column(int),
            }

            operator = DataFrameSchemaOperator(
                task_id="test1", dataframe=single_column_dataframe, columns=columns
            )

            operator.execute()

    def test_operator_two_column_df_success(self, two_column_dataframe):

        columns = {"column1": Column(str), "column2": Column(int)}

        operator = DataFrameSchemaOperator(
            task_id="test1", dataframe=two_column_dataframe, columns=columns
        )

        operator.execute()

    def test_operator_two_column_df_fail(self, two_column_dataframe):

        with pytest.raises(SchemaError):

            columns = {"column1": Column(int), "column2": Column(str)}

            operator = DataFrameSchemaOperator(
                task_id="test1", dataframe=two_column_dataframe, columns=columns
            )

            operator.execute()
