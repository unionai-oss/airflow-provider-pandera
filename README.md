# Apache Airflow Provider for Pandera

Airflow operators that interface with [Pandera](https://github.com/unionai-oss/pandera).

## Installation

Pre-requisites:
    - pandera
    - apache-airflow

```bash
pip install airflow-provider-pandera
```

## Usage

There are several ways that you can use the PanderaOperator. 

The simplest use case is passing a dataframe object directly from a different task as an XCom to the PanderaOperator.

You can provide either a `columns` dictionary with the schema to be validated by a DataFrameSchema object or you
can provide a SchemaModel class.

Beware that in order to pass a dataframe object between tasks using XComs you need to enable the `enable_xcom_pickling`
option in your `airflow.cfg` file.

For examples on how to use the operator, checkout `pandera_provider/sample_dags/`.

## Modules

Currently there is only one operator, the PanderaOperator.
`from pandera_provider.operators.pandera import PanderaOperator`

Stub doc: [Here](https://www.notion.so/Design-Doc-Airflow-Pandera-Provider-a352cc3c49844a0dbacff16ba40ff079)

## Contributing

```bash
git clone https://github.com/unionai-oss/airflow-provider-pandera
cd airflow-provider-pandera
python -m venv venv
. venv/bin/activate
pip install -e .
pip install -r requirements-dev.txt
```

To run the tests
```bash
make test
```
