import datetime

import pendulum
import pytest
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from pandas import DataFrame
from pandera.errors import SchemaError

DATA_INTERVAL_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=2)


class TestPanderaOperatorDataFrameSchema:
    def test_simple_df(
        self,
        dataframeschema_simple_df_dag,
    ):
        dagrun = dataframeschema_simple_df_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = dataframeschema_simple_df_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="pandera_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                ti.run(ignore_ti_state=True)
                assert ti.state == TaskInstanceState.SUCCESS

    def test_wrong_xcom_key(
        self,
        dataframeschema_wrong_xcom_key_dag,
    ):
        dagrun = dataframeschema_wrong_xcom_key_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = dataframeschema_wrong_xcom_key_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="wrong_key"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(ValueError):
                    ti.run(ignore_ti_state=True)
                    assert ti.state == TaskInstanceState.FAILED

    def test_empty_df(
        self,
        dataframeschema_empty_df_dag,
    ):
        dagrun = dataframeschema_empty_df_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = dataframeschema_empty_df_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="pandera_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(ValueError):
                    ti.run(ignore_ti_state=True)
                    assert ti.state == TaskInstanceState.FAILED

    def test_wrong_schema_df(
        self,
        dataframeschema_wrong_schema_df_dag,
    ):
        dagrun = dataframeschema_wrong_schema_df_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = dataframeschema_wrong_schema_df_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="pandera_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(SchemaError):
                    ti.run(ignore_ti_state=True)
                    assert ti.state == TaskInstanceState.FAILED


class TestPanderaOperatorSchemaModel:
    def test_simple_df(
        self,
        schemamodel_simple_df_dag,
    ):
        dagrun = schemamodel_simple_df_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = schemamodel_simple_df_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="pandera_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                ti.run(ignore_ti_state=True)
                assert ti.state == TaskInstanceState.SUCCESS

    def test_wrong_xcom_key(
        self,
        schemamodel_wrong_xcom_key_dag,
    ):
        dagrun = schemamodel_wrong_xcom_key_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = schemamodel_wrong_xcom_key_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="wrong_key"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(ValueError):
                    ti.run(ignore_ti_state=True)
                    assert ti.state == TaskInstanceState.FAILED

    def test_empty_df(
        self,
        schemamodel_empty_df_dag,
    ):
        dagrun = schemamodel_empty_df_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = schemamodel_empty_df_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="pandera_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(ValueError):
                    ti.run(ignore_ti_state=True)
                    assert ti.state == TaskInstanceState.FAILED

    def test_wrong_schema_df(
        self,
        schemamodel_wrong_schema_df_dag,
    ):
        dagrun = schemamodel_wrong_schema_df_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = schemamodel_wrong_schema_df_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "df_generator_task":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="pandera_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(SchemaError):
                    ti.run(ignore_ti_state=True)
                    assert ti.state == TaskInstanceState.FAILED
