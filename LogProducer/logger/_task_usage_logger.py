# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: _task_usage_logger.py
# project name: LogProducer
# date: 24/11/2020
from logger._logger import Logger
from logger._record import TaskUsageRecord
from iostream import TaskUsageLoader
from config import LoggerConfig


class TaskUsageLogger(Logger):

    __instance__ = None

    def __init__(self, config: LoggerConfig):
        if TaskUsageLogger.__instance__ is None:
            super().__init__()
            self.__config = config
            self._loader = TaskUsageLoader()
            self._recorder = TaskUsageRecord()
            TaskUsageLogger.__instance__ = self
        else:
            raise Exception("Cannot create another instance of {}".format(self.__class__))

    @classmethod
    def get_instance(cls, config: LoggerConfig):
        if cls.__instance__ is None:
            TaskUsageLogger(config)
        return cls.__instance__

    # def _loader(self) -> TaskUsageLoader:
    #     return self.__loader
    #
    # def _recorder(self) -> TaskUsageRecord:
    #     return self.__recorder
    #
    # @property
    # def config(self):
    #     return self.__config
