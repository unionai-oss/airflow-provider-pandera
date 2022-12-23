# Apache Airflow Provider for Pandera

The airflow-provider-pandera is an addon package for Apache Airflow that provides
an operator (PanderaOperator) for validating dataframes using [Pandera](https://github.com/unionai-oss/pandera).

## Installation

Pre-requisites:
- apache-airflow
- pandera

### Pip
To install the airflow-provider-pandera operator you can run the following command:
```bash
$ pip install airflow-provider-pandera
```

## Usage
Currently there are some different ways that you can use the PanderaOperator for validating dataframes.
You can use the DataSchemaModel or the SchemaModel objects.

You need to specify one of the two when using the operator. If both, or none are specified, the operator will
raise a `ValueError` when attempting to run the task.

### Using DataFrameSchema

```python
from pandera_provider.operators.pandera import PanderaOperator
from pandera import Column, DataFrameSchema

validate_dataframe_task = PanderaOperator(
    task_id="validate_dataframe_task",
    dataframeschema=DataFrameSchema(
        columns={
            "col1": Column(str)
        }
    ),
)
```

### Using SchemaModel

```python
from pandera_provider.operators.pandera import PanderaOperator
from pandera import SchemaModel
from pandera.typing import Series

# You can create your Schema class wherever you want in your project and import
# it using standard Python imports or declare it directly in the DAG file.
class Schema(SchemaModel):

    col1: Series[str]

validate_dataframe_task = PanderaOperator(
    task_id="validate_dataframe_task",
    schemamodel=Schema,
)
```

## Passing data to the operator

### Reading data from csv files

```python
from pandera_provider.operators.pandera import PanderaOperator
from pandera import Column, DataFrameSchema


validate_dataframe_task = PanderaOperator(
    filepath="path/to/local/csv_file.csv",
    task_id="validate_dataframe_task",
    dataframeschema=DataFrameSchema(
        columns={
            "col1": Column(str)
        }
    ),
)
```

### Reading data from XCOM
```python
df_generator_task = PythonOperator(
    dataframe_xcom_key="dataframe_for_pandera",
    task_id="df_generator_task",
    python_callable=lambda **kwargs: kwargs["ti"].xcom_push(
        key="pandera_df", value=DataFrame({"col1": ["test"]}),
    ),
)

# The above is equivalent to the following:
# def generate_dataframe(**kwargs):
#     ti = kwargs["ti"]
#     df = DataFrame({"col1": ["test"]})
#     ti.xcom_push("dataframe_for_pandera", df)
# 
# df_generator_task = PythonOperator(
#     task_id="df_generator_task",
#     python_callable=generate_dataframe,
# )

validate_dataframe_task = PanderaOperator(
    task_id="validate_dataframe_task",
    dataframeschema=DataFrameSchema(
        columns={
            "col1": Column(str)
        }
    ),
)
```

For complete dag examples, check: [pandera_provider/example_dags](https://github.com/erichamers/airflow-provider-pandera/tree/main/pandera_provider/example_dags)

## Modules

Currently there is only one operator, the PanderaOperator.
`from pandera_provider.operators.pandera import PanderaOperator`

[Stub doc](https://www.notion.so/Design-Doc-Airflow-Pandera-Provider-a352cc3c49844a0dbacff16ba40ff079)

## Contributing

```bash
# Clone this repository
$ git clone https://github.com/erichamers/airflow-provider-pandera
$ cd airflow-provider-pandera

# Create the virtual environment
$ python -m venv venv

# Activate the virtual environment
$ . venv/bin/activate

# Install the package
$ pip install -e .

# Install the development dependencies
$ pip install -r requirements-dev.txt
```

To run the tests
```bash
# Run the tests using the Makefile
$ make test

# Or you can run the command directly
$ airflow db reset -y && pytest
```
