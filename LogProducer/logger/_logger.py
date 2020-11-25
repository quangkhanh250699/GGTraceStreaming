# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _logger.py
# project name: LogProducer
# date: 24/11/2020

from abc import ABCMeta, abstractmethod
from typing import List

from logger._record import Record
from iostream import Loader
from config import LoggerConfig
from timer import Timer


class Logger(metaclass=ABCMeta):

    __instance__ = None

    def __init__(self):
        self.__checked_time = 0
        self.__current_time = 0
        Timer.get_instance()

    @classmethod
    @abstractmethod
    def get_instance(cls, config: LoggerConfig):
        pass

    def log(self) -> List[str]:
        self._update(self.__current_time)
        pre, cur = self._get_interval()
        payloads = self._loader().load(pre, cur)
        return self._recorder().to_record_strings(payloads)

    @abstractmethod
    def _loader(self) -> Loader:
        pass

    @abstractmethod
    def _recorder(self) -> Record:
        pass

    def _get_interval(self):
        return self.__checked_time, self.__current_time

    def _update(self, pre):
        self.__checked_time = pre
        self.__current_time = Timer.check()
