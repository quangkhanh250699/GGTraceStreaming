# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _loader.py
# project name: LogProducer
# date: 24/11/2020

from abc import ABCMeta, abstractmethod
from typing import List


class Loader(metaclass=ABCMeta):

    @abstractmethod
    def _connection(self):
        pass

    @abstractmethod
    def _get_event_time(self, line: str) -> float:
        pass

    def _get_last_event(self) -> (float, str):
        try:
            return self.__last_event_time, self.__last_event
        except AttributeError as e:
            return -1, ''

    def _set_last_event(self, time: float, event: str):
        self.__last_event_time = time
        self.__last_event = event

    def load(self, pre: float, cur: float) -> List[str]:
        payloads = list()
        # If the last event checked is far from this moment
        last_time, event = self._get_last_event()
        if last_time > cur:
            return payloads
        else:
            if last_time >= pre:
                payloads.append(event)
        try:
            while True:
                line = self._connection().readline()
                event_time = self._get_event_time(line)
                if pre < event_time <= cur:
                    payloads.append(line)
                else:
                    self._set_last_event(event_time, line)
                    break
        except FileExistsError as e:
            print(e)
        except Exception as e:
            print(e)

        return payloads
