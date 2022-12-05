from pandera import DataFrameSchema
from airflow.models import BaseOperator


class DataFrameSchemaOperator(BaseOperator):
    def __init__(
        self,
        dataframe=None,
        columns=None,
        checks=None,
        index=None,
        dtype=None,
        coerce=False,
        strict=False,
        name=None,
        ordered=False,
        unique=None,
        report_duplicates="all",
        unique_column_names=False,
        title=None,
        description=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.columns = columns
        self.checks = checks
        self.index = index
        self.dtype = dtype
        self.coerce = coerce
        self.strict = strict
        self.name = name
        self.ordered = ordered
        self.unique = unique
        self.report_duplicates = report_duplicates
        self.unique_column_names = unique_column_names
        self.title = title
        self.description = description

    def execute(self):
        schema = DataFrameSchema()
        validated_df = schema(self.dataframe)
        return validated_df
