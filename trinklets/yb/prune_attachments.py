import logging
import json
import sys
import os
from pathlib import Path
from typing import Any, Dict, List

import requests
import utils
from main.config import Config
from main.report import Report
from pyhocon import ConfigFactory
import datetime

LOGGER = logging.getLogger()


def find_vcs_root(test=".", dirs=(".git",), default=".") -> str:
    prev, test = None, os.path.abspath(test)
    while prev != test:
        if any(os.path.isdir(os.path.join(test, d)) for d in dirs):
            return test
        prev, test = test, os.path.abspath(os.path.join(test, os.pardir))
    return default


def root_dir():
    return find_vcs_root()


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
    log_dir = Path(root_dir(), "logs")
    log_dir.mkdir(exist_ok=True)
    log_file_path = Path(log_dir).joinpath("app_debug.log")
    fh = logging.FileHandler(log_file_path, mode="w")

    fh_formatter = logging.Formatter(
        "%(name)s:%(filename)s:%(funcName)s:%(lineno)d:%(levelname)-8s:: %(message)s"
    )
    fh.setFormatter(fh_formatter)
    logging.getLogger().addHandler(fh)


class ReportPlus(Report):

    def get_tests(self, suite_name: str) -> List[Dict[str, Any]]:
        start_time = datetime.datetime.now() - datetime.timedelta(days=100)
        payload = {
            "start": start_time.timestamp(),
            "end": datetime.datetime.now().timestamp(),
            "filters": [{"key": "suite_name", "value": suite_name}],
        }
        resp = requests.post(f"{self.url}/back/get_tests", json=payload, timeout=300)
        res = []
        if resp.status_code == 200:
            res = resp.json()["tests"]
        else:
            raise RuntimeError(
                "Resp status code: %s, %s", str(resp.status_code), resp.text
            )
        return res

    def get_attachments(self, test_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = {"test_id": test_info.get("test_id")}
        LOGGER.debug("Payload is %s", str(payload))
        resp = requests.post(
            f"{self.url}/back/get_attachments", json=payload, timeout=300
        )
        res = []
        if resp.status_code == 200:
            res = resp.json()["attachments"]
        else:
            raise RuntimeError(
                "Resp status code: %s, %s", str(resp.status_code), resp.text
            )
        return res

    def delete_attachment(self, attachment_info: Dict[str, Any]):
        payload = {"attachments": [attachment_info]}
        LOGGER.debug("Payload is %s", str(payload))
        resp = requests.post(
            f"{self.url}/back/delete_attachments", json=payload, timeout=300
        )
        res = {}
        if resp.status_code == 200:
            LOGGER.info(f"attachment deleted: {attachment_info['attachment_id']}")
            res = resp.json()
        else:
            raise RuntimeError(
                "Resp status code: %s, %s", str(resp.status_code), resp.text
            )
        return res


def filter_attachment_by_name(
    attachment_info_list: List[Dict[str, Any]], name
) -> List[Dict[str, Any]]:
    flattened_list = []
    for attachment_info in attachment_info_list:
        if name in attachment_info["name"]:
            if len(attachment_info.get("children", [])) == 0:
                flattened_list.append(attachment_info)
            else:
                LOGGER.info(
                    "Found tpch_data.gz with child nodes!: %s",
                    json.dumps(attachment_info),
                )
        else:
            flattened_list.extend(
                filter_attachment_by_name(attachment_info.get("children", []), name)
            )
    return flattened_list


if __name__ == "__main__":

    init_logger()

    # create instance of reporter
    config_path = "/Users/arastogi/code/yb-stress-test/src/configs/default.conf"
    hakon_config = ConfigFactory.parse_file(config_path)
    raw_config = utils.dict_merge({}, utils.dict_through(hakon_config))
    config = Config(raw_config=raw_config)
    report = ReportPlus(config)

    # get all tests
    tests = report.get_tests(suite_name="ParallelQuerySuite")

    all_attachments: List[Dict[str, Any]] = []
    total_tests = len(tests)
    for index, test_info in enumerate(tests):
        LOGGER.info(f"Fetching attachments: {index} of {total_tests}")
        attachment_info = report.get_attachments(test_info=test_info)
        all_attachments.extend(attachment_info)

    attachments_to_prune = filter_attachment_by_name(
        all_attachments, name="tpch_data.gz"
    )

    # filter attachments older than 2 weeks
    attachments_to_prune_2_weeks = []
    two_weeks_before = datetime.datetime.now() - datetime.timedelta(weeks=2)
    for att in attachments_to_prune:
        d1 = datetime.datetime.fromtimestamp(att["time"])
        if d1 < two_weeks_before:
            attachments_to_prune_2_weeks.append(att)

    # delete the attachment
    start_index = 1
    index = start_index
    while index < len(attachments_to_prune_2_weeks):
        LOGGER.info(
            f"Delete attachment: {index} of {len(attachments_to_prune_2_weeks)}"
        )
        report.delete_attachment(attachments_to_prune_2_weeks[index])
        start_index = index
        index += 1
