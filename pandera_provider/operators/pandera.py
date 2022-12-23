from typing import Any, Dict, Optional, Tuple

from airflow.models import BaseOperator
from pandas import DataFrame, read_csv
from pandas.errors import EmptyDataError
from pandera import DataFrameSchema, SchemaModel
from pandera.errors import SchemaError


class PanderaOperator(BaseOperator):
    """
    An operator for validating a DataFrame using pandera.

    Currently there are two different ways that you can validate a dataframe
    using the PanderaOperator.

    You can validate it using the DataFrameSchema object. Or you can validate
    it using the SchemaModel object.

    To select which one you want to use, you need to pass it to the operator
    via the `dataframeschema` or `schema_model` parameters.

    You can pass one and only one of those parameters. If both are specified
    the operator will raise a ValueError. Also, if none are specified, the
    operator will raise a ValueError.

    At this point, there are two ways that you can feed a dataframe for the
    operator to validate. Using XCOMs or giving it a file path for a local .csv
    file.

    To pass a DataFrame object via XCOM, you can specify the key for the XCOM
    you're defining via the `dataframe_xcom_key` parameter, or you can leave it
    blank and it'll be set to the default value (pandera_df).

    One caveat when passing dataframe objects as XCOMs, is that you need to set
    the configuration `enable_xcom_pickling=True` in the airflow.cfg file.
    """

    def __init__(
        self,
        filepath: Optional[str] = None,
        dataframe_xcom_key: Optional[str] = "pandera_df",
        dataframeschema: Optional[DataFrameSchema] = None,
        schema_model: Optional[SchemaModel] = None,
        fail_task_on_validation_failure: Optional[bool] = True,
        return_validation_dataframe: Optional[bool] = False,
        *args,
        **kwargs,
    ) -> None:
        """
        Args:
            dataframeschema (Optional[DataFrameSchema], optional):
        DataFrameSchema object to be used when validating the dataframe.
        Defaults to None.
            schema_model (Optional[SchemaModel], optional):
        The SchemaModel object to be used when validating the dataframe.
        Defaults to None.
            fail_task_on_validation_failure (Optional[bool], optional):
        If this is set to true, the validation task status will be set to False
        even if the validation fails. Defaults to True.
            return_validation_dataframe (Optional[bool], optional):
        If True, this will return the resulting dataframe from the validation
        step. Defaults to False.

        Raises:
            ValueError: _description_
        """
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.dataframe_xcom_key = dataframe_xcom_key
        self.dataframeschema = dataframeschema
        self.schema_model = schema_model
        self.schema = (
            self.dataframeschema if self.dataframeschema else self.schema_model
        )
        self.fail_task_on_validation_failure = fail_task_on_validation_failure
        self.return_validation_dataframe = return_validation_dataframe

        if not (bool(self.dataframeschema) ^ bool(self.schema_model)):
            raise ValueError(
                "Exactly one of `dataframeschema` or `schema_model` may be "
                "specified."
            )

    def check_fail_task_on_validation_failure(
        self, dataframe: DataFrame, error: str
    ) -> None:
        """
        Checks if the `check_fail_task_on_validation_failure` flag is True. If
        it is, it will throw a warning message if the validation step fails.
        But it will mark the DAG as successfull if the other tasks are
        successfull.

        Args:
            dataframe (DataFrame): Dataframe to validate.
            error (str): The error message to be displayed.

        Raises:
            SchemaError: Pandera error when a validation fails.
        """
        if self.fail_task_on_validation_failure:
            raise SchemaError(data=dataframe, message=error, schema=self.schema)
        else:
            self.log.warning(
                "Validation with Pandera failed. "
                "Continuing DAG execution because "
                "fail_task_on_validation_failure is set to False."
            )

    def validate(self, dataframe: DataFrame) -> Tuple[DataFrame, None]:
        """
        Validates the dataframe

        Args:
            dataframe (DataFrame): Dataframe to be validated.

        Returns:
            Tuple[DataFrame, None]: If the validation fails, it will throw an
        error and return None, otherwise, it returns the resulting DataFrame
        from pandera's validate method.
        """
        try:
            if self.return_validation_dataframe:
                return self.schema.validate(dataframe)
            else:
                self.schema.validate(dataframe)
        except SchemaError as e:
            self.check_fail_task_on_validation_failure(dataframe, e)
        except Exception as e:
            self.check_fail_task_on_validation_failure(dataframe, e)

    def execute(self, context: Dict[str, Any]) -> Tuple[DataFrame, None]:
        """
        Executes the operator

        Args:
            context (Dict[str, Any]): Context provided by Airflow
        """
        if not self.filepath:
            dataframe = context["ti"].xcom_pull(key=self.dataframe_xcom_key)
            if dataframe is None:
                raise ValueError(
                    f"Couldn't find an XCOM with the key `{self.dataframe_xcom_key}`"
                )
        else:
            try:
                dataframe = read_csv(self.filepath)
            except EmptyDataError as e:
                raise ValueError(e)

        if dataframe.empty:
            raise ValueError("Cannot validate an empty dataframe.")

        return self.validate(dataframe)
