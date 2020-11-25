# author: Khanh.Quang 
# institute: Hanoi University of Science and Technology
# file name: log_manager.py
# project name: LogProducer
# date: 24/11/2020

from connector import Connector, KafkaConnector
from config import ConnectorConfig, LoggerConfig
from logger import TaskUsageLogger, Logger
from timer import Timer

from typing import List, Tuple, Set
import time


def _get_connector(config: ConnectorConfig):
    name = config.name
    if name == "KAFKA_CONNECTOR":
        return KafkaConnector(config)
    else:
        raise Exception("Not found type of connector!")


def _get_logger(config: LoggerConfig):
    name = config.name
    if name == "TASK_USAGE":
        return TaskUsageLogger.get_instance(config)
    else:
        raise Exception("Not found type of logger config named {}".format(name))


class LogManager:
    __logger_configs: List[LoggerConfig]
    __connector_configs: List[ConnectorConfig]
    __loggers: List[Logger]
    __connectors: List[Connector]
    __logger_types: Set

    __interval_log: float
    __interval_waiting: float

    def __init__(self, configs: List[Tuple[LoggerConfig, ConnectorConfig]],
                 interval_log: 5,
                 interval_waiting: 2):
        self.__interval_log = interval_log
        self.__interval_waiting = interval_waiting
        Timer(self.__interval_log)
        self.__logger_configs = [config[0] for config in configs]
        self.__connector_configs = [config[1] for config in configs]
        self.__loggers = list()
        self.__connectors = list()
        self.__logger_types = set()
        self.__set_log_and_connection()

    def dispatch_logs(self):
        while True:
            clock = Timer.clock()
            for i in range(len(self.__loggers)):
                payload = self.__loggers[i].log()
                print(payload.__len__())
                self.__connectors[i].submit(payload)
            time.sleep(self.__interval_waiting)

    def __set_log_and_connection(self):
        for i in range(len(self.__logger_configs)):
            logger_config = self.__logger_configs[i]
            connector_config = self.__connector_configs[i]
            name = logger_config.name
            if name in self.__logger_types:
                continue
            self.__logger_types.add(name)
            self.__loggers.append(_get_logger(logger_config))
            self.__connectors.append(_get_connector(connector_config))


if __name__ == '__main__':
    task_usage_logger_config = LoggerConfig("TASK_USAGE", 300)
    kafka_connector_config = ConnectorConfig("KAFKA_CONNECTOR", "TASK-USAGE")
    manager = LogManager([(task_usage_logger_config, kafka_connector_config)], 100, 5)
    manager.dispatch_logs()
