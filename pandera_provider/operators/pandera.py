from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from pandera import DataFrameSchema, SchemaModel


class PanderaOperator(BaseOperator):
    """
    An operator for validating a DataFrame using pandera.

    In order to use SchemaModel for validating the dataframe, you need to pass
    the SchemaModel object to the `schema_model` parameter.

    To use the DataFrameSchema for validating the dataframe, you need to pass
    the DataFrameSchema object to the `dataframeschema` parameter.

    *When pushing the XCom object, it's key need to be set to 'pandera_df'*

    In order to pass non json objects via xcom you need to change the following
    setting in airflow.cfg:

    `enable_xcom_pickling = True`

    """

    def __init__(
        self,
        dataframeschema: Optional[DataFrameSchema] = None,
        schema_model: Optional[SchemaModel] = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Args:
            dataframeschema (Optional[DataFrameSchema], optional):
                A DataFrameSchema object to validate the dataframe. Defaults to None.
            schema_model (Optional[SchemaModel], optional):
                A SchemaModel object to validate the dataframe. Defaults to None.

        Raises:
            ValueError: _description_
        """
        super().__init__(*args, **kwargs)
        self.dataframeschema = dataframeschema
        self.schema_model: Optional[SchemaModel] = schema_model

        if not (bool(self.dataframeschema) ^ bool(self.schema_model)):
            raise ValueError(
                "Exactly one of `dataframeschema` or `schema_model` may be specified."
            )

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Runs the operator

        Args:
            context (Dict[str, Any]): Context provided by Airflow
        """
        dataframe = context["ti"].xcom_pull(key="pandera_df")

        try:
            if not bool(dataframe):
                raise ValueError("Couldn't find an XCom with key `pandera_df`")
        except (AttributeError, ValueError):
            if dataframe.empty:
                raise ValueError("Got an empty dataframe in XCom `pandera_df`.")

        if self.dataframeschema:
            schema = self.dataframeschema
        elif self.schema_model:
            schema = self.schema_model
        results = schema.validate(dataframe)
        return results
