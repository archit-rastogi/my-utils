from abc import abstractmethod
from functools import partial
from threading import Thread
from typing import Callable, List, Optional

from apps.tpcc.tpcch_app import TPCHApp
from apps.yugabyte.yugabyte_abstract_app import AbstractYugabyteApp
from apps.yugaware.types import YWNodeDetailsSet
from main.config import HostConfig
from main.lstbench.models import TaskType
from main.lstbench.runner import LstTask


class TpchBaseTask(LstTask):

    def __init__(self, task_type: TaskType, tpch_app: TPCHApp, yb: AbstractYugabyteApp):
        super().__init__(task_type=task_type)
        self.app = tpch_app
        self.yb = yb
        self.thread = None

    def run(self, run_on_host: HostConfig):
        node_details: List[YWNodeDetailsSet] = self.yw.get_universe_details().details.node_details
        target_hosts = [node.cloud_info.private_ip for node in node_details]
        target = self.get_runnable_target(run_on_host, target_hosts)
        self.thread = Thread(target=target)
        self.thread.start()

    @abstractmethod
    def get_runnable_target(self, run_on_host, target_hosts) -> Callable[[], None]:
        pass

    def wait(self, timeout: Optional[float] = 0):
        if self.thread:
            self.thread.join(timeout)


class TpchAppLoadTask(TpchBaseTask):

    def __init__(self, tpch_app: TPCHApp, yb: AbstractYugabyteApp):
        super().__init__(TaskType.LOAD, tpch_app, yb)

    def get_runnable_target(self, run_on_host: HostConfig, target_hosts: List[str]) -> Callable[[], None]:
        return partial(self.app.run_workload,
                       run_on_host=run_on_host.private_ip,
                       database_name="yb1",
                       target_host=target_hosts,
                       run_queries=False
                       )


class TpchAppSingleUserTask(TpchBaseTask):

    def __init__(self, tpch_app: TPCHApp, yb: AbstractYugabyteApp):
        super().__init__(TaskType.SINGLE_USER, tpch_app, yb)

    def get_runnable_target(self, run_on_host: HostConfig, target_hosts: List[str]) -> Callable[[], None]:
        return partial(self.app.run_queries,
                       database_name="yb1",
                       target_host=target_hosts)


class TpchAppDataMaintenceTask(TpchBaseTask):

    # TODO: run inserts/updates/deletes

    def __init__(self, tpch_app: TPCHApp, yb: AbstractYugabyteApp):
        super().__init__(TaskType.DATA_MAINTENANCE, tpch_app, yb)

    def get_runnable_target(self, run_on_host: HostConfig, target_hosts: List[str]) -> Callable[[], None]:
        return partial(self.app.run_queries,
                       database_name="yb1",
                       target_host=target_hosts)
