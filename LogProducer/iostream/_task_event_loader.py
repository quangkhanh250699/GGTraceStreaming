# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _task_event_loader.py
# project name: LogProducer
# date: 25/11/2020

from iostream._loader import Loader

import re


class TaskEventLoader(Loader):
    __source = "data/task_events/part-00001-of-00500.csv"

    def __init__(self):
        self.__file = open(self.__source)

    def _connection(self):
        return self.__file

    def _get_event_time(self, line: str) -> float:
        result = re.match("([0-9]*),([0-9]*),(.*)", line)
        if result is None:
            return -1
        else:
            return int(result.group(1))
