# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from log_manager import LogManager
from logger import TaskUsageLogger
from connector import KafkaConnector
from config import LoggerConfig, ConnectorConfig


class Main:
    task_usage_logger_config = LoggerConfig("TASK_USAGE", 100)
    task_usage_connector_config = ConnectorConfig("KAFKA_CONNECTOR", "TASK-USAGE")
    task_event_logger_config = LoggerConfig("TASK_EVENT", 100)
    task_event_connector_config = ConnectorConfig("KAFKA_CONNECTOR", "TASK-EVENT")
    __log_manager = LogManager([(task_usage_logger_config, task_usage_connector_config),
                                (task_event_logger_config, task_event_connector_config)],
                               100, 5)

    @classmethod
    def run(cls):
        cls.__log_manager.dispatch_logs()


if __name__ == '__main__':
    app = Main()
    app.run()
