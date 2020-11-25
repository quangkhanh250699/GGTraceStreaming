# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from log_manager import LogManager
from logger import TaskUsageLogger
from connector import KafkaConnector
from config import LoggerConfig, ConnectorConfig

class Main:

    __log_manager: LogManager

    def __init__(self):
        task_usage_logger_config = LoggerConfig("TASK_USAGE")
        kafka_connector_config = ConnectorConfig("KAFKA_CONNECTOR", "TASK_USAGE")
        self.__log_manager = LogManager([(task_usage_logger_config, kafka_connector_config)])

    def run(self):
        pass
