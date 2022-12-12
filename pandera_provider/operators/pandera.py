from typing import Any, Dict

from airflow.models import BaseOperator
from pandera import DataFrameSchema, SchemaModel


class PanderaOperator(BaseOperator):
    """
    An operator for validating a DataFrame using pandera.

    In order to use SchemaModel for validating the dataframe, you need to pass
    the SchemaModel object to the `schema_model` parameter.

    To use the DataFrameSchema for validating the dataframe, you need to pass
    a dictionary containing the column mappings to the `columns` parameter.

    *When pushing the XCom object, it's key need to be set to 'dfs_operator_df'*

    In order to pass non json objects via xcom you need to change the following
    setting in airflow.cfg:

    `enable_xcom_pickling = True`

    Methods
    ------
    execute(context={})
        Executes the operator.
    """

    def __init__(
        self,
        columns: dict = None,
        schema_model: SchemaModel = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        columns: dict
            A dictionary containing the mapping between columns and types.

        schema_model: SchemaModel
            A schema model object to validate the dataframe.
        """
        super().__init__(*args, **kwargs)
        self.columns = columns
        self.schema_model = schema_model
        self.__dict__.update(kwargs)

        if not self.columns and not self.schema_model:
            raise ValueError(
                "Need to provide columns or schema_model parameters to the operator"
            )

        if self.columns and self.schema_model:
            raise ValueError(
                "Both columns and schema_model parameters were set. Can only have one."
            )

    def execute(self, context: Dict[str, Any] = {}) -> Any:
        """
        Runs the operator.

        Parameters
        ---------
        context: dict
            Context provided by Airflow.
        """
        df = context["ti"].xcom_pull(key="dfs_operator_df")

        if self.columns:
            schema = DataFrameSchema(columns=self.columns)
            results = schema.validate(df)
            return results

        if self.schema_model:
            schema = self.schema_model
            results = schema.validate(df)
            return results
