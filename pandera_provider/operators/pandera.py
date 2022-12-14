from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from pandas import DataFrame
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
    -------
    execute(context={})
        Executes the operator.
    """

    def __init__(
        self,
        columns: Optional[Dict[str, Any]] = None,
        schema_model: Optional[SchemaModel] = None,
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
        self.columns: Optional[Dict[str, Any]] = columns
        self.schema_model: Optional[SchemaModel] = schema_model

        if bool(self.columns) and not isinstance(self.columns, Dict):
            raise TypeError(
                f"Expected `self.columns` to be of type `dict` but got {type(self.columns)} instead"
            )

        # If columns is specified, but it's an empty dictionary, raises ValueError.

        if not (bool(self.columns) ^ bool(self.schema_model)):
            raise ValueError("Exactly one of columns or schema_model may be specified.")

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Runs the operator.

        Parameters
        ----------
        context: dict
            Context provided by Airflow.
        """
        dataframe = context["ti"].xcom_pull(key="dfs_operator_df")

        try:
            if not bool(dataframe):
                raise ValueError("Couldn't find an XCom with key `dfs_operator_df`")
        except (AttributeError, ValueError):
            if dataframe.empty:
                raise ValueError("Got an empty dataframe in XCom `df_operator_df`.")

        if isinstance(dataframe, DataFrame):

            schema = (
                DataFrameSchema(columns=self.columns)
                if self.columns
                else self.schema_model
                if self.schema_model
                else None
            )

            if self.columns:
                schema = DataFrameSchema(columns=self.columns)
            elif self.schema_model:
                schema = self.schema_model

            results = schema.validate(dataframe)
            return results
