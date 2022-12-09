import numpy as np
import pytest

from pandas import DataFrame


@pytest.fixture
def single_column_dataframe():

    df = DataFrame({"column1": ["pandera", "is", "awesome"]})
    return df


@pytest.fixture
def double_column_dataframe():

    df = DataFrame({"column1": ["pandera", "is", "awesome"], "column2": [1, 2, 3]})
    return df


@pytest.fixture
def triple_column_dataframe():

    df = DataFrame(
        {
            "column1": ["pandera", "is", "awesome"],
            "column2": [1, 2, 3],
            "column3": [0.1, 0.2, 0.3],
        }
    )
    return df


@pytest.fixture
def triple_column_dataframe_with_null():

    df = DataFrame(
        {
            "column1": ["pandera", "is", "awesome"],
            "column2": [1, 2, 3],
            "column3": [0.1, 0.2, np.nan],
        }
    )
    return df
