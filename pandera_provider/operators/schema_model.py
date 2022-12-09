from typing import Any, Dict

from airflow.models import BaseOperator
from pandas import DataFrame
from pandera import SchemaModel


class SchemaModelOperator(BaseOperator):
    """
    Provides an operator for running validations on the data using the
    SchemaModel.

    Methods
    ------
    execute(context={})
        Executes the operator.
    """

    def __init__(
        self, dataframe: DataFrame, schema_model: SchemaModel, *args, **kwargs
    ) -> None:
        """
        Parameters
        ----------
        dataframe: DataFrame
            A dataframe object
        schema_model: SchemaModel
            A SchemaModel object containing the mapping for the columns in the
            dataframe.
        """
        super.__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.schema_model = schema_model

    def execute(self, context: Dict[str, Any] = {}) -> Any:
        """
        Runs the operator.

        Parameters
        ---------
        context: dict
            Context provided by Airflow.
        """
        results = self.schema_model.validate(self.dataframe)
        return results
