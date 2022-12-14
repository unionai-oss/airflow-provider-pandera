import datetime

import pendulum
import pytest
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from pandas import DataFrame
from pandera.errors import SchemaError

from tests.fixtures import (
    dataframe,
    dataframeschema_fail_dag,
    dataframeschema_success_dag,
)


class TestPanderaOperatorDataFrameSchema:
    DATA_INTERVAL_START = pendulum.datetime(2021, 9, 13, tz="UTC")
    DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=2)

    def test_pandera_operator_using_dataframeschema_success(
        self,
        dataframeschema_success_dag,
    ):

        dagrun = dataframeschema_success_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=self.DATA_INTERVAL_START,
            data_interval=(self.DATA_INTERVAL_START, self.DATA_INTERVAL_END),
            start_date=self.DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = dataframeschema_success_dag.get_task(task_id=ti.task_id)
            ti.run(ignore_ti_state=True)
            if ti.task_id == "dfs_operator_df":
                assert isinstance(ti.xcom_pull(key="dfs_operator_df"), DataFrame)
            assert ti.state == TaskInstanceState.SUCCESS

    def test_pandera_operator_using_dataframeschema_fail(
        self, dataframeschema_fail_dag
    ):

        dagrun = dataframeschema_fail_dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=self.DATA_INTERVAL_START,
            data_interval=(self.DATA_INTERVAL_START, self.DATA_INTERVAL_END),
            start_date=self.DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        tis = dagrun.get_task_instances()

        for ti in tis:
            ti.task = dataframeschema_fail_dag.get_task(task_id=ti.task_id)
            if ti.task_id == "dfs_operator_df":
                ti.run(ignore_ti_state=True)
                assert isinstance(ti.xcom_pull(key="dfs_operator_df"), DataFrame)
                assert ti.state == TaskInstanceState.SUCCESS
            else:
                with pytest.raises(SchemaError):
                    ti.run(ignore_ti_state=True)
                assert ti.state == TaskInstanceState.FAILED
