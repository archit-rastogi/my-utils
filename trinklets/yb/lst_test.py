import logging
import os
import sys
from pathlib import Path
from uuid import uuid4

from pyhocon import ConfigFactory

import utils
from apps.client.client_server import ClientServer, ClientServerNode
from apps.lst.yb_long_system_test_app import YbLongSystemTestApp
from main.artifacats import Artifact
from main.config import Config, HostConfig, HostType


def find_vcs_root(test=".", dirs=(".git",), default=None) -> str:
    prev, test = None, os.path.abspath(test)
    while prev != test:
        if any(os.path.isdir(os.path.join(test, d)) for d in dirs):
            return test
        prev, test = test, os.path.abspath(os.path.join(test, os.pardir))
    return default


def root_dir():
    return find_vcs_root(".")


def init_logger(log_level: int = logging.DEBUG):
    # setup logging
    logging.basicConfig(
        filename="app.log",
        level=log_level,
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    # set up logging to console
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(filename)s:%(lineno)d %(levelname)s: %(message)s")
    console.setFormatter(formatter)

    # add the handler to the root logger
    logging.getLogger().addHandler(console)

    # also set up file handler and capture all debug logs
    log_dir = Path(root_dir()).joinpath("logs")
    log_dir.mkdir(exist_ok=True)
    log_file_path = Path(log_dir).joinpath("app_debug.log")
    fh = logging.FileHandler(log_file_path, mode="w")

    fh_formatter = logging.Formatter(
        "%(name)s:%(filename)s:%(funcName)s:%(lineno)d:%(levelname)-8s:: %(message)s"
    )
    fh.setFormatter(fh_formatter)
    logging.getLogger().addHandler(fh)


if __name__ == '__main__':

    init_logger()
    logger = logging.getLogger()

    config_path = "/Users/arastogi/code/yb-stress-test/src/configs/default.conf"
    hakon_config = ConfigFactory.parse_file(config_path)
    raw_config = utils.dict_merge({}, utils.dict_through(hakon_config))
    config = Config(raw_config=raw_config)
    config.meta.logger = logger

    # the client machine in mac here.
    run_on_host = "172.151.17.103"
    base_dir = "/tmp/tests/artifacts/yugabyte-yb-long-system-test-75d8d2fe0b0520276b135a8912efc9ae38da1322"
    artifact_1 = Artifact(config, artifact_name='yb-long-system-test')
    
    app = YbLongSystemTestApp(config, artifact_1)
    app.artifact.artifact_remote_dir[run_on_host] = f"{base_dir}"    

    host = HostConfig()
    host.host_type = HostType.CLIENT
    host.username = "ec2-user"
    host.key_path = "/Users/arastogi/.yugabyte/yb-dev-aws-2.pem"
    host.port = 22
    host.remote_home_path = "/tmp/tests"
    host.use_internal_download = True
    host.env_vars = {}
    host.proxy = None
    host.tags = {}
    host.private_ip = run_on_host
    host.public_ip = run_on_host
    host.instance_id = ""

    config.init_connection()
    app.ssh.add_connection(host)
    config.update_hosts([host], remove_home=False)
    artifact_1.deploy()
    env_dict = app.ssh.clients[run_on_host].config.env_vars
    env_dict["PYENV_ROOT"] = "$HOME/.pyenv"
    env_dict["PATH"] = "$PYENV_ROOT/bin:$PATH"

    # client_server on local
    artifact = Artifact(config, artifact_name='stress-client-server')
    artifact.artifact_remote_path[run_on_host] = f"{base_dir}/yb-stress-test/tools/client-server/target/" + \
        "yb-stress-client-server.jar"
    client_server: ClientServer = ClientServer(config, artifact)
    node_id = str(uuid4())
    client_server.nodes[node_id] = ClientServerNode(
        id=node_id,
        pid=None,
        host=config.meta.client_private_hosts[0],
        log_path=f"config.meta.remote_temp_dir/client-server-{node_id[:6]}.log",
        port=8080)
    app.client_server = client_server

    # test client server
    res = client_server.execute(
        queries=["select count(*) from person;"],
        keyspace='yugabyte',
        username='yugabyte',
        password="",
        addresses="localhost:5433")
    print(res)

    # build_and_init app for mac
    # set executable path for tpc-h app on mac

    app.run_workload(run_on_host=run_on_host, database_name="yb2", target_host=["localhost:5433"], yb=None,
                     run_queries=False)

    app.analyze_tables(target_host=["localhost:5433"], database_name="yb2", username="yugabyte", password="", yb=None)

    # step 1: generate data for updates and deletes
    # datasets = app.generate_data_for_update_and_delete(run_on_host=run_on_host, partition_count=1, split_files=1)
    datasets = app.generate_data_for_update_and_delete(run_on_host=run_on_host, partition_count=5, split_files=10)

    app.remove_update_and_delete_datasets(run_on_host=run_on_host)

    # step 2: run dml
    app.run_dml(run_on_host=run_on_host, database_name="yb2", file_list=datasets,
                target_host=["localhost:5433"], yb=None, username="yugabyte", password="")

    # get queries
    queries = app.get_queries(run_on_host, "yb2")
