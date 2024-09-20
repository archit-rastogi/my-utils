import logging
from pathlib import Path

import requests
from pyhocon import ConfigFactory

import utils
from main.config import Config
from main.report import Report, ResultsType

if __name__ == '__main__':

    config_path = "/Users/arastogi/code/yb-stress-test/src/configs/default.conf"
    hakon_config = ConfigFactory.parse_file(config_path)
    raw_config = utils.dict_merge({}, utils.dict_through(hakon_config))
    config = Config(raw_config=raw_config)

    reporter = Report(config)
    reporter.logger = logging.getLogger()
    reporter.add_results(name="Just checking", data="some error", result_type=ResultsType.EXCEPTION)

    # attaching
    reporter.add_local_attachment_file(
        "Framework logs/reports/example.html",
        Path("/Users/arastogi/code/yb-stress-test/src/example.html")
    )

    # downloading
    # get attachment details
    payload = {"test_id": reporter.latest_test_id}
    resp = requests.post(f"{reporter.url}/back/get_attachments", json=payload, timeout=120)
    details = resp.json()

    # use file server to download attachments
    params = {
        "name": "31a333d9-4f58-457d-85c1-4dca947f940a-client-server-a10983.log"
    }
    resp = requests.get(url=f"{reporter.url}/files/get", params=params, timeout=120)
    with open("/Users/arastogi/code/test.log", "wb") as fp:
        fp.write(resp.content)

    # find all tests in report
    resp = requests.post(url=f"{reporter.url}/back/get_reports", timeout=120)
    all_reports = resp.json()
    report_names = [rp["name"] for rp in all_reports["reports"]]
    index = report_names.index("Parallel Query")
    pq_report = all_reports["reports"][index]
    """
    {
  "report_id": "a98c3f0a-88c3-44d9-bf35-055a54803db5",
  "name": "Parallel Query",
  "config": {
    "tag": "parallel_query",
    "pages": {
      "2.23.0.0-b18": {
        "order": 3013,
        "statuses": {
          "failed": 1
        }
      },
      "2.23.0.0-b51": {
        "order": 3019,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b52": {
        "order": 3020,
        "statuses": {
          "failed": 1
        }
      },
      "2.23.0.0-b59": {
        "order": 3021,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b70": {
        "order": 3022,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b85": {
        "order": 3025,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b91": {
        "order": 3026,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b99": {
        "order": 3028,
        "statuses": {
          "passed": 2
        }
      },
      "2.21.1.0-b124": {
        "order": 2001,
        "statuses": {
          "failed": 4,
          "passed": 5
        }
      },
      "2.21.1.0-b133": {
        "order": 2010,
        "statuses": {
          "passed": 1
        }
      },
      "2.21.1.0-b208": {
        "order": 2011,
        "statuses": {
          "failed": 14,
          "passed": 8
        }
      },
      "2.21.1.0-b250": {
        "order": 2037,
        "statuses": {
          "failed": 1,
          "passed": 1
        }
      },
      "2.23.0.0-b106": {
        "order": 3031,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b122": {
        "order": 3032,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b131": {
        "order": 3033,
        "statuses": {
          "failed": 1
        }
      },
      "2.23.0.0-b141": {
        "order": 3034,
        "statuses": {
          "failed": 1,
          "passed": 6
        }
      },
      "2.23.0.0-b143": {
        "order": 3035,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b157": {
        "order": 3043,
        "statuses": {
          "passed": 1
        }
      },
      "2.23.0.0-b169": {
        "order": 3037,
        "statuses": {
          "failed": 1
        }
      },
      "2024.1.0.0-b6": {
        "order": 3012,
        "statuses": {
          "failed": 1
        }
      },
      "2024.1.0.0-b14": {
        "order": 3014,
        "statuses": {
          "failed": 2
        }
      },
      "2024.1.0.0-b17": {
        "order": 3017,
        "statuses": {
          "failed": 1
        }
      },
      "2024.1.0.0-b28": {
        "order": 3024,
        "statuses": {
          "passed": 1
        }
      },
      "2024.1.0.0-b41": {
        "order": 3030,
        "statuses": {
          "passed": 1
        }
      },
      "2024.1.0.0-b14-arm": {
        "order": 3015,
        "statuses": {
          "failed": 1
        }
      }
    },
    "update_pages": 1713340303.197334
  },
  "creation_time": 1708498391.891523
}
    """

    # Get all results from the test and ingest in sqlite3 or duckdb ?
    params = {
        "name": "Parallel Query",
        "page": "2.23.0.0-b244"
    }
    resp = requests.post(url=f"{reporter.url}/back/get_report_tests", json=params, timeout=120)
    # >>> resp.json()["tests"][0].keys()
    # dict_keys(['test_id', 'config', 'start_time', 'end_time', 'status'])
    # >>> len(resp.json()["tests"])
    # 1

    payload = {
        "test_id": "ff4a80ab-62ab-4bc6-a3f2-088fc2b7605d"
    }
    resp = requests.post(url=f"{reporter.url}/back/get_test_results", json=payload, timeout=120)
    # {'results': [{'result_id': '7ba7ff0f-3bcd-4f21-b855-04ff27944caf', 'data': [['Query Name', 'Without Parallel',
    # 'With Parallel Workers', 'Better', 'Verify Records'], ['16.sql', {'value': '2.06', 'status': 'passed'},
    # {'value': '2.09', 'status': 'passed'}, {'value': '1.456310679611641', 'status': 'failed'}, {'value': 'EQ',
    # 'status': 'passed'}], ['12.sql', {'value': '32.04', 'status': 'passed'}, {'value': '17.03', 'status': 'passed'},
    # {'value': '46.84769038701622', 'status': 'passed'}, {'value': 'EQ', 'status': 'passed'}]],
    # 'name': 'Compare Average Duration - Without Nemesis', 'test_id': '1b3254e0-d985-40fb-8b67-99f77c37596e',
    # 'type': 'table'}], 'status': True}

    # ^^ similarly /back/add_test_results

    # get test info
    payload = {
        "test_id": "ff4a80ab-62ab-4bc6-a3f2-088fc2b7605d"
    }
    resp = requests.post(url=f"{reporter.url}/back/get_test_info", json=payload, timeout=120)
    # >>> resp.json().keys()
    # dict_keys(['test_info', 'status'])
    # >>> resp.json()["test_info"].keys()
    # dict_keys(['test_id', 'config', 'start_time', 'end_time', 'status'])

    # download all artifacts and Ingest all log records in sqlite3 db
    # Run queries and publish results

    # get report pages
    params = {
        "name": "Parallel Query",
        #    "page": "2024.1.0-b41-arm"
    }
    resp = requests.post(url=f"{reporter.url}/back/get_report_pages", json=params, timeout=120)
    """
    resp = {
        "pages": {
            "<page_name>": {
                "order": 3046,
                "passed": 1,
                "failed": 2
            }
        }
    }
    """
