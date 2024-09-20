import logging
import random
import time

from main.lstbench.runner import LstTask

LOGGER = logging.getLogger(__name__)


class Task1(LstTask):

    def run(self, run_on_host):
        # sleep for random time
        rtime = random.randint(5, 30)
        total_time = 0
        while total_time < rtime:
            time.sleep(2)
            total_time += 2
            LOGGER.info("Slept for %d secs of %d", total_time, rtime)

    def wait(self, timeout=0):
        pass
