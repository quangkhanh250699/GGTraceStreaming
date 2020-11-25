# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _task_usage_load.py
# project name: LogProducer
# date: 25/11/2020

from iostream._loader import Loader

import re


class TaskUsageLoader(Loader):
    __source = "data/task_usage/part-00001-of-00500.csv"

    def __init__(self):
        self.__file = open(self.__source)

    def _connection(self):
        return self.__file

    def _get_event_time(self, line: str) -> float:
        result = re.match("([0-9]*),([0-9]*),(.*)", line)
        if result is None:
            return 0
        else:
            return float(result.group(1)) / 1000000 - 5500
